import dataAppHtml from "../dist/index.html?raw";
import seedSnapshot from "./data.json";
import { dataAppOwnerUserIdSha256, dataAppOwnerEmailSha256 } from "./data-app-owner.js";
import { createDataAppWorker } from "./data-app-worker.js";

export default createDataAppWorker({
  html: dataAppHtml,
  seedSnapshot,
  ownerUserIdSha256: dataAppOwnerUserIdSha256,
  ownerEmailSha256: dataAppOwnerEmailSha256,
});
