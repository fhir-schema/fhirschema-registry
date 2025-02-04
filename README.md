# FHIR Artifact repository

## Basic cli

* Install package with deps for example r4 & terminology.r4
* Generate fhir-schemas
* Resolve deps for fhir schemas
* Generate type schemas

far hl7.fhir.core.r4 -> type-schemas.ndjson.gz
sdk-get type-schemas.ndjson.gz -> types

## TODO:


;; deps management

;; canonicals table with package_id
;; package_deps (all deps ids + level)
;; canonical_deps table with source (canonical_id), type and path and url/version

;; after providing list canonicals
;; we can resolve all deps
;; and make uber-package with everything inside

;;as well we can check that all deps are resolvable

First do it for SD, VS

SD deps:
- baseDefinition
- type
- binding
- reference

VS
- codesystem
- valueset




