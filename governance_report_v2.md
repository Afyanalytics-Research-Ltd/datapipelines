# Orthopedic V2 — PII Classification Report (machine-suggested)

Generated: 2026-07-08T12:05:51.954872+00:00
connection_id=20  facility_id=47

**This is a SUGGESTION, not an approval.** Every model stays gated (`approved: false`) in `pii_classification_v2.json` until a human reviews its field list below — especially anything under NEEDS REVIEW — and flips `approved` to `true`. `orthopedic_v2_clean_pipeline.py` will refuse to process a model that isn't approved.

## admnotes  (App\Models\Admonotes)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| pid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| description | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| username | DIRECT_IDENTIFIER | HASH |
| date | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| time | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |

## cadex  (App\Models\Cadex)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| pid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| patid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| patname | DIRECT_IDENTIFIER | HASH |
| date | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| stamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| fullname | DIRECT_IDENTIFIER | HASH |
| notes | CLINICAL_CONTENT | KEEP |
| time | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| username | DIRECT_IDENTIFIER | HASH |
| status | SYSTEM_META | KEEP |
| mainid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |

## history  (App\Models\History)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| pid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| description | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| username | DIRECT_IDENTIFIER | HASH |
| date | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| time | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| age | QUASI_IDENTIFIER | KEEP |
| gender | QUASI_IDENTIFIER | KEEP |
| stamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| mainbranch | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |

## icd10diagnosis  (App\Models\ICD10diagnosis)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| pid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| stamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| code | SYSTEM_META | KEEP |
| doctor | STAFF_IDENTIFIER | KEEP |
| maincateg | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| time | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| pattype | SYSTEM_META | KEEP |
| refin | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| refout | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| age | QUASI_IDENTIFIER | KEEP |
| mainbranch | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| diseasename | DIRECT_IDENTIFIER | HASH |
| diseasetype | SYSTEM_META | KEEP |
| gender | QUASI_IDENTIFIER | KEEP |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |

## icd10diseases  (App\Models\ICD10diseases)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| code | SYSTEM_META | KEEP |
| BlockId | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| name | DIRECT_IDENTIFIER | HASH |
| ClassKind | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| IsResidual | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| PrimaryLocation | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| chapter | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Grouping1 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Grouping2 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| maincateg | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| diseases_category | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| disease_class | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| disease_sub_class | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |

## impression  (App\Models\Impression)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| pid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| description | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| username | DIRECT_IDENTIFIER | HASH |
| date | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| time | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| age | QUASI_IDENTIFIER | KEEP |
| gender | QUASI_IDENTIFIER | KEEP |
| stamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| mainbranch | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |

## inpatients  (App\Models\Inpatients)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| Id | SYSTEM_META | KEEP |
| PrescId | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| TransDate | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| PatId | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| PatName | DIRECT_IDENTIFIER | HASH |
| MainWard | QUASI_IDENTIFIER | KEEP |
| Admitted | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| WardType | SYSTEM_META | KEEP |
| RoomNo | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| BedNo | SYSTEM_META | KEEP |
| AdmDate | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Discharge | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Doctor | STAFF_IDENTIFIER | KEEP |
| AdmNotes | CLINICAL_CONTENT | KEEP |
| Status | SYSTEM_META | KEEP |
| Stamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| PayMode | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| DiagCat | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| DiagName | DIRECT_IDENTIFIER | HASH |
| Deposit | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Impression | CLINICAL_CONTENT | KEEP |
| AdmittedBy | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| BedCategory | SYSTEM_META | KEEP |
| DischargeStamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| DischargeTime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| DischargedBy | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| DischargeNotes | CLINICAL_CONTENT | KEEP |
| IpNo | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Diagnosis | CLINICAL_CONTENT | KEEP |
| Icd10 | CLINICAL_CONTENT | KEEP |
| Type | SYSTEM_META | KEEP |
| DischargeCateg | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| BedId | SYSTEM_META | KEEP |
| mainbranch | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| RadiologyNotes | CLINICAL_CONTENT | KEEP |
| ExpectedStay | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| PayVisitId | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| LongStayExplanation | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Age | QUASI_IDENTIFIER | KEEP |
| Gender | QUASI_IDENTIFIER | KEEP |
| LastActive | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| TriageStatus | CLINICAL_CONTENT | KEEP |
| LastBedCharge | SYSTEM_META | KEEP |
| ClosedBill | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| LimitNotification | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| AutoBill | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| LodgerFee | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| LodgerName | DIRECT_IDENTIFIER | HASH |
| LodgerShip | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| LodgerPhone | UNKNOWN | HASH ⚠️ NEEDS REVIEW |

## labrequests  (App\Models\Labrequests)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| labno | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| prescid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| patid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| patname | DIRECT_IDENTIFIER | HASH |
| section | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| request | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| results | CLINICAL_CONTENT | KEEP |
| reqtime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| rectime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| servtime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| date | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| stamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| requested | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| doneby | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| status | SYSTEM_META | KEEP |
| rcptno | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| paid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| sample | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| sample_condition | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| accept_reject | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| source | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| other_details | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| unique_no | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| restype | SYSTEM_META | KEEP |
| age | QUASI_IDENTIFIER | KEEP |
| sex | QUASI_IDENTIFIER | KEEP |
| weight | CLINICAL_CONTENT | KEEP |
| impression | CLINICAL_CONTENT | KEEP |
| verifiedby | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| mainbranch | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| reqstamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| recstamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| servstamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| billed | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| notes | CLINICAL_CONTENT | KEEP |
| collstamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| eqcode | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| critical | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| labware_status | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |

## labtestresults  (App\Models\Labtestresults)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| reqid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| subtestname | DIRECT_IDENTIFIER | HASH |
| results | CLINICAL_CONTENT | KEEP |
| normalrange | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| units | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| type | SYSTEM_META | KEEP |
| categories | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| specifics | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| result2 | CLINICAL_CONTENT | KEEP |
| heading | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| flagdescription | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| critical_low | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| critical_high | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |

## newprescription  (App\Models\Newprescription)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| Id | SYSTEM_META | KEEP |
| PrescId | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| TransDate | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Type | SYSTEM_META | KEEP |
| PatId | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| OpNo | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| IpNo | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Temp | CLINICAL_CONTENT | KEEP |
| Bp1 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Bp2 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Weight | CLINICAL_CONTENT | KEEP |
| Height | CLINICAL_CONTENT | KEEP |
| BMI | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| RespRate | CLINICAL_CONTENT | KEEP |
| PulseRate | CLINICAL_CONTENT | KEEP |
| Rbs | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| SP02 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Muac | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Allergies | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| OtherDetails | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| History | CLINICAL_CONTENT | KEEP |
| Complaint | CLINICAL_CONTENT | KEEP |
| PhyExam | CLINICAL_CONTENT | KEEP |
| Reclab | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| LabTests | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| LabResults | CLINICAL_CONTENT | KEEP |
| Recrad | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| RadTests | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| RadiologyResults | CLINICAL_CONTENT | KEEP |
| Treatment | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| RecTheatre | CLINICAL_CONTENT | KEEP |
| Surgery | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Diagnosis | CLINICAL_CONTENT | KEEP |
| Prescription | CLINICAL_CONTENT | KEEP |
| Admitted | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| WardType | SYSTEM_META | KEEP |
| RoomNo | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| BedNo | SYSTEM_META | KEEP |
| AdmDate | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Discharge | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Doctor | STAFF_IDENTIFIER | KEEP |
| ProgressNotes | CLINICAL_CONTENT | KEEP |
| DoctorNotes | STAFF_IDENTIFIER | KEEP |
| NursesNotes | STAFF_IDENTIFIER | KEEP |
| RecPharm | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| RecNurse | STAFF_IDENTIFIER | KEEP |
| Status | SYSTEM_META | KEEP |
| Stamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| StartTime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| TriageTime | CLINICAL_CONTENT | KEEP |
| ConsTime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| LabTime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| RadTime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| PharmTime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Dept | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| DiagCat | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| DiagName | DIRECT_IDENTIFIER | HASH |
| DayCare | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| InsId | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| ReviewDate | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| DocStatus | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Icd10 | CLINICAL_CONTENT | KEEP |
| TriageStatus | CLINICAL_CONTENT | KEEP |
| Impression | CLINICAL_CONTENT | KEEP |
| PatCat | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| TimeStamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| SickOff | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| RefIn | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| RefOut | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Age | QUASI_IDENTIFIER | KEEP |
| Gender | QUASI_IDENTIFIER | KEEP |
| Waiver | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Exemption | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Insurance | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| RTA | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Tb1 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Tb2 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Tb3 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Tb4 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Tb5 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| VisitNo | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| ClinSumm | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| Police | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| mainbranch | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| PayVisitId | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| ShiftId | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| RcptNo | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| LetterHead | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| RefCateg | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| RefHosp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| RefRemarks | CLINICAL_CONTENT | KEEP |
| AppointRemarks | CLINICAL_CONTENT | KEEP |
| RadiologyNotes | CLINICAL_CONTENT | KEEP |
| UrineProtein | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| MainSymptoms | CLINICAL_CONTENT | KEEP |
| MainPE | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| MicroAlbumin | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| InProgress | CLINICAL_CONTENT | KEEP |
| ZScore | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| BMI4Age | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| VitaminA | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| FileType | SYSTEM_META | KEEP |
| UnseenRemarks | CLINICAL_CONTENT | KEEP |
| VisitConfirm | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| AppointmentId | UNKNOWN | HASH ⚠️ NEEDS REVIEW |

## patients  (App\Models\Patientsmodel)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| pntno | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |

## pharmrequests  (App\Models\Pharmrequests)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| prescid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| patid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| prescription | CLINICAL_CONTENT | KEEP |
| reqtime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| servtime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| date | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| stamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| status | SYSTEM_META | KEEP |
| dispstatus | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| dispensed | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| recnurse | STAFF_IDENTIFIER | KEEP |
| recnursestatus | STAFF_IDENTIFIER | KEEP |
| paid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| rcptno | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| itcode | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| itname | DIRECT_IDENTIFIER | HASH |
| qty | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| strength | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| dosage1 | CLINICAL_CONTENT | KEEP |
| dosage2 | CLINICAL_CONTENT | KEEP |
| route | CLINICAL_CONTENT | KEEP |
| frequency | CLINICAL_CONTENT | KEEP |
| duration1 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| duration2 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| prescno | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| apsite | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| age | QUASI_IDENTIFIER | KEEP |
| gender | QUASI_IDENTIFIER | KEEP |
| weight | CLINICAL_CONTENT | KEEP |
| drugcateg | CLINICAL_CONTENT | KEEP |
| visitno | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| icd10 | CLINICAL_CONTENT | KEEP |
| price | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| category | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| mainbranch | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| adminstatus | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| notes | CLINICAL_CONTENT | KEEP |
| billed | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| directions | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| instructions | CLINICAL_CONTENT | KEEP |
| formula_id | SYSTEM_META | KEEP |
| formula_name | DIRECT_IDENTIFIER | HASH |
| billtime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| billstamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| sale_id | SYSTEM_META | KEEP |
| dispensedstamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| source | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| sourcecat | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| isPrescription | CLINICAL_CONTENT | KEEP |
| ref_doctor | STAFF_IDENTIFIER | KEEP |
| ref_facility | SYSTEM_META | KEEP |
| isRefill | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| currentRefill | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| maxRefills | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| batches | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| nextDate | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| nextDateStamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| lastDate | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| userstore | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |

## phyexam  (App\Models\phyexam)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| pid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| description | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| date | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| time | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |

## physical  (App\Models\Physical)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| pid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| a1 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| b1 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| a2 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| b2 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| a3 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| b3 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| a4 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| b4 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| a5 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| b5 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| a6 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| b6 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| a7 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| b7 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| a8 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| b8 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| a9 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| b9 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| a10 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| b10 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| a11 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| b11 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| a12 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| b12 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| a13 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| b13 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| date | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| time | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |

## procrequests  (App\Models\Procrequests)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| procno | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| prescid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| patid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| request | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| results | CLINICAL_CONTENT | KEEP |
| reqtime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| servtime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| date | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| stamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| status | SYSTEM_META | KEEP |
| rcptno | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| paid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| mainbranch | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |

## progress  (App\Models\Progress)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| pid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| patid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| date | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| stamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| notes | CLINICAL_CONTENT | KEEP |
| time | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |

## radrequests  (App\Models\Radrequests)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| radno | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| prescid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| patid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| request | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| results | CLINICAL_CONTENT | KEEP |
| reqtime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| servtime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| date | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| stamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| status | SYSTEM_META | KEEP |
| rcptno | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| paid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| mainbranch | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| notes | CLINICAL_CONTENT | KEEP |
| rectime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| recstamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| billed | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |

## theatrequests  (App\Models\Theatrequests)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| theatreno | CLINICAL_CONTENT | KEEP |
| prescid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| patid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| procedures | CLINICAL_CONTENT | KEEP |
| indication | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| sedation | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| findings | CLINICAL_CONTENT | KEEP |
| recommendations | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| image | DIRECT_IDENTIFIER | HASH |
| reqtime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| servtime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| date | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| stamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| status | SYSTEM_META | KEEP |
| rcptno | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| paid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| age | QUASI_IDENTIFIER | KEEP |
| gender | QUASI_IDENTIFIER | KEEP |
| approach | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| procnotes | CLINICAL_CONTENT | KEEP |
| complications | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| extraproc | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| tissue | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| prosthesis | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| closure | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| operdate | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| opertime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| type | SYSTEM_META | KEEP |
| mainbranch | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| category | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| specialty | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| itcode | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| itname | DIRECT_IDENTIFIER | HASH |
| amount | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| payvisitid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| planned | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| planned_time | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| source | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| admitted | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| deposit | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| approved | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| room | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| booked | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| wound_site | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| consciousness | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| request_notes | CLINICAL_CONTENT | KEEP |
| consent | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| consenttimestamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| macroid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| macroname | DIRECT_IDENTIFIER | HASH |
| equipment | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| startstamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| startdate | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| starttime | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| asa | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| macrodetailsuser | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| chartparams | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| preoparams | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| anaesthesiatype | SYSTEM_META | KEEP |
| premed | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| postopparams | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |

## triage  (App\Models\Triage)
- approved: **False**
- field source: describe

| field | category | action |
|---|---|---|
| id | SYSTEM_META | KEEP |
| pid | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| temp | CLINICAL_CONTENT | KEEP |
| bp1 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| bp2 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| weight | CLINICAL_CONTENT | KEEP |
| height | CLINICAL_CONTENT | KEEP |
| bmi | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| resp | CLINICAL_CONTENT | KEEP |
| pulse | CLINICAL_CONTENT | KEEP |
| sp02 | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| rbs | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| muac | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| date | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| stamp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| time | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| lmp | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| ldd | UNKNOWN | HASH ⚠️ NEEDS REVIEW |
| created_at | SYSTEM_META | KEEP |
| updated_at | SYSTEM_META | KEEP |
| deleted_at | SYSTEM_META | KEEP |
