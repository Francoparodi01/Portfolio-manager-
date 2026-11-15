"""Resolve public CNV presentation attachments; a loading page is not a filing."""
import json
import re
from urllib.parse import urljoin, urlsplit
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup

from src.thesis_desk.contracts import check_url
from src.thesis_desk.sources import SourceUnavailable

CNV_HOSTS = ["www.cnv.gov.ar", "aif2.cnv.gov.ar", "aif.cnv.gov.ar", "blob.cnv.gov.ar"]


def unescape_js_literal(value):
    escapes = {"n": "\n", "r": "\r", "t": "\t", "b": "\b", "f": "\f", "'": "'", '"': '"', "\\": "\\", "/": "/"}
    def replace(match):
        token = match[0][1:]
        if token.startswith(("u", "x")):
            return chr(int(token[1:], 16))
        if token not in escapes:
            raise ValueError("Unsupported CNV literal escape")
        return escapes[token]
    return re.sub(r"\\(?:u[0-9a-fA-F]{4}|x[0-9a-fA-F]{2}|.)", replace, value)


def cnv_attachments(body, presentation_url):
    check_url(presentation_url, CNV_HOSTS)
    soup = BeautifulSoup(body, "html.parser")
    scripts = "\n".join(tag.get_text() for tag in soup.select("script:not([src])"))
    model = re.search(r"\bvar\s+presentation\s*=\s*'((?:\\.|[^'\\])*)'\s*;", scripts)
    host = re.search(r'\bvar\s+blobHost\s*=\s*"([^"\s]+)"', scripts)
    if not model or not host:
        raise ValueError("CNV presentation has no public attachment metadata")
    check_url(host[1], ["blob.cnv.gov.ar"])
    try:
        tree = ET.fromstring(unescape_js_literal(model[1]))
    except ET.ParseError as exc:
        raise ValueError("Invalid CNV presentation XML") from exc
    parts = urlsplit(presentation_url)
    origin = parts.scheme + "://" + parts.netloc
    attachments = {}
    for prop in tree.iter("propiedad"):
        if not prop.get("uploader"):
            continue
        for item in json.loads(prop.text or "[]"):
            guid = item.get("guid", "")
            if not re.fullmatch(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", guid):
                raise ValueError("Invalid CNV attachment identity")
            attachment = dict(guid=guid, filename=item["nombreArchivo"], publisher_attachment_hash=item.get("hash"),
                publisher_hash_basis="UNSPECIFIED_BY_SOURCE_NOT_ASSUMED_SHA256",
                source_url=urljoin(host[1], "DownloadBlob/" + guid),
                public_grant_url=origin + "/api/ValetKeyProvider/GetPublicValetKey/" + guid + "?operation=DownloadBlob",
                presentation_url=presentation_url, presentation_title=soup.title.get_text(" ", strip=True) if soup.title else None)
            if guid in attachments and attachments[guid] != attachment:
                raise ValueError("Conflicting CNV attachment metadata")
            attachments[guid] = attachment
    if not attachments:
        raise ValueError("CNV presentation has no attachments")
    return list(attachments.values())


def public_blob_loader(attachment, max_bytes):
    """The public site's POST downloads a file; it does not create a presentation.

    The short-lived public download grant stays in memory and out of receipts/logs.
    """
    async def download(client):
        check_url(attachment["public_grant_url"], CNV_HOSTS)
        check_url(attachment["source_url"], ["blob.cnv.gov.ar"])
        response = await client.get(attachment["public_grant_url"])
        if response.status_code != 200:
            raise SourceUnavailable(f"CNV_PUBLIC_GRANT_HTTP_{response.status_code}")
        token = response.json().get("valetKeyData")
        if not isinstance(token, str) or not token:
            raise ValueError("CNV public download grant unavailable")
        async with client.stream("POST", attachment["source_url"], data={"ValetKey": token}) as response:
            if response.status_code != 200:
                raise SourceUnavailable(f"CNV_DOWNLOAD_HTTP_{response.status_code}")
            chunks, size = [], 0
            async for chunk in response.aiter_bytes():
                size += len(chunk)
                if size > max_bytes:
                    raise SourceUnavailable("SOURCE_SIZE_LIMIT")
                chunks.append(chunk)
        body = b"".join(chunks)
        if not body.startswith(b"%PDF"):
            raise ValueError("CNV attachment is not a supported PDF")
        return body
    return download
