// Sites packaging selects exactly one verified owner identity. Prefer the
// stable, Site-scoped user ID. The email seed is a temporary compatibility
// mode for an older backend and must never supplement a user-ID seed.
// Unseeded or ambiguously seeded apps stay read-only. Repackage after an
// ownership change; an unchanged Worker cannot observe a transfer.
export const dataAppOwnerUserIdSha256 = "";
export const dataAppOwnerEmailSha256 = "";
