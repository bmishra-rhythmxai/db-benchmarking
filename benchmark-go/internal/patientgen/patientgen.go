package patientgen

import (
	"encoding/json"
	"fmt"
	"math/rand"
)

const (
	payloadPoolSize = 100
	payloadSize     = 2 * 1024 * 1024 // 2 MiB
)

var payloadPool []string

func init() {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	payloadPool = make([]string, payloadPoolSize)
	for i := 0; i < payloadPoolSize; i++ {
		b := make([]byte, payloadSize)
		for j := range b {
			b[j] = letters[rand.Intn(len(letters))]
		}
		payloadPool[i] = string(b)
	}
}

// PatientRecord is a single patient record for load.
type PatientRecord struct {
	IsOriginal               bool        `json:"is_original"`
	FHIRID                   string      `json:"FHIR_ID"`
	RXPatientID              string      `json:"RX_PATIENT_ID"`
	Source                   string      `json:"SOURCE"`
	CDC                      interface{} `json:"CDC"`
	CreatedAt                interface{} `json:"CREATED_AT"`
	CreatedBy                interface{} `json:"CREATED_BY"`
	UpdatedAt                interface{} `json:"UPDATED_AT"`
	UpdatedBy                interface{} `json:"UPDATED_BY"`
	LoadDate                 interface{} `json:"LOAD_DATE"`
	Checksum                 interface{} `json:"CHECKSUM"`
	PatientID                string      `json:"PATIENT_ID"`
	MedicalRecordNumber      string      `json:"MEDICAL_RECORD_NUMBER"`
	NamePrefix               string      `json:"NAME_PREFIX"`
	LastName                 string      `json:"LAST_NAME"`
	FirstName                string      `json:"FIRST_NAME"`
	NameSuffix               interface{} `json:"NAME_SUFFIX"`
	DateOfBirth              string      `json:"DATE_OF_BIRTH"`
	GenderAdministrative     string      `json:"GENDER_ADMINISTRATIVE"`
	FHIRGenderAdministrative string      `json:"FHIR_GENDER_ADMINISTRATIVE"`
	GenderIdentity           string      `json:"GENDER_IDENTITY"`
	FHIRGenderIdentity       string      `json:"FHIR_GENDER_IDENTITY"`
	MaritalStatus            string      `json:"MARITAL_STATUS"`
	FHIRMaritalStatus        string      `json:"FHIR_MARITAL_STATUS"`
	RaceDisplay              string      `json:"RACE_DISPLAY"`
	FHIRRaceDisplay          string      `json:"FHIR_RACE_DISPLAY"`
	EthnicityDisplay         string      `json:"ETHNICITY_DISPLAY"`
	FHIREthnicityDisplay     string      `json:"FHIR_ETHNICITY_DISPLAY"`
	SexAtBirth               string      `json:"SEX_AT_BIRTH"`
	IsPregnant               string      `json:"IS_PREGNANT"`
}

var firstNames = []string{"John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"}
var lastNames = []string{"Smith", "Doe", "Brown", "Johnson", "Williams", "Jones", "Garcia", "Miller", "Davis", "Wilson"}
var genders = []string{"male", "female", "other"}

// GenerateBulkPatients generates total patient records with duplicates.
// start is the starting counter for MRN/patient IDs. duplicateRatio (e.g. 0.25) controls duplicates.
func GenerateBulkPatients(start, total int, duplicateRatio float64) []PatientRecord {
	nUnique := total - int(float64(total)*duplicateRatio)
	if nUnique < 1 {
		nUnique = 1
	}
	patients := make([]PatientRecord, 0, total)
	baseSource := payloadPool[rand.Intn(len(payloadPool))]

	for i := 0; i < nUnique; i++ {
		ordinal := formatOrdinal(start + i)
		mrn := "MRN-" + ordinal
		pid := "patient-" + ordinal
		namePrefix := "Mr"
		if i%2 != 0 {
			namePrefix = "Ms"
		}
		nameSuffix := interface{}(nil)
		if i%4 == 0 {
			nameSuffix = "Jr"
		}
		patients = append(patients, PatientRecord{
			IsOriginal:               true,
			FHIRID:                   pid,
			RXPatientID:              "rx-" + pid,
			Source:                   baseSource,
			PatientID:                pid,
			MedicalRecordNumber:      mrn,
			NamePrefix:               namePrefix,
			LastName:                 lastNames[i%len(lastNames)],
			FirstName:                firstNames[i%len(firstNames)],
			NameSuffix:               nameSuffix,
			DateOfBirth:              formatDateOfBirth(1980+(i%40), (i%12)+1, (i%28)+1),
			GenderAdministrative:     genders[i%3],
			FHIRGenderAdministrative: genders[i%3],
			GenderIdentity:           capitalize(genders[i%3]),
			FHIRGenderIdentity:       genders[i%3],
			MaritalStatus:            boolToMarital(i%2 == 0),
			FHIRMaritalStatus:        boolToFHIRMarital(i%2 == 0),
			RaceDisplay:              boolToRace(i%3 == 0),
			FHIRRaceDisplay:          boolToFHIRRace(i%3 == 0),
			EthnicityDisplay:         "Not Hispanic or Latino",
			FHIREthnicityDisplay:     "2186-5",
			SexAtBirth:               boolToSex(i%2 == 0),
			IsPregnant:               "false",
		})
	}

	nDuplicates := total - nUnique
	for j := 0; j < nDuplicates; j++ {
		dup := patients[j%nUnique]
		dup.IsOriginal = false
		dup.Source = baseSource
		patients = append(patients, dup)
	}
	return patients
}

func formatOrdinal(n int) string {
	if n < 0 {
		n = 0
	}
	return fmt.Sprintf("%010d", n)
}

func formatDateOfBirth(y, m, d int) string {
	return fmt.Sprintf("%d-%02d-%02d", y, m, d)
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	b := []byte(s)
	if b[0] >= 'a' && b[0] <= 'z' {
		b[0] -= 'a' - 'A'
	}
	return string(b)
}

func boolToMarital(married bool) string {
	if married {
		return "Married"
	}
	return "Single"
}

func boolToFHIRMarital(married bool) string {
	if married {
		return "M"
	}
	return "S"
}

func boolToRace(white bool) string {
	if white {
		return "White"
	}
	return "Black or African American"
}

func boolToFHIRRace(white bool) string {
	if white {
		return "2106-3"
	}
	return "2054-5"
}

func boolToSex(female bool) string {
	if female {
		return "female"
	}
	return "male"
}

// ToJSON returns the record as JSON for the message body.
func (p PatientRecord) ToJSON() (string, error) {
	b, err := json.Marshal(p)
	return string(b), err
}
