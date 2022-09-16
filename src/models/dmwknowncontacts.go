package models

type DmwKnownContacts struct {
	Contacts []DmwKnownContact `json:"contacts"`
	Error    error
}

type DmwKnownContact struct {
	ContactID                   int64  `json:"ContactID"`
	MasterContactID             int64  `json:"MasterContactID"`
	TenantID                    string `json:"TenantID"`
	QueueID                     string `json:"QueueID,omitempty"`
	StartDate                   string `json:"StartDate"`
	FromAddr                    string `json:"FromAddr"`
	CurrentContactState         int32  `json:"CurrentContactState"`
	CurrentContactDate          string `json:"CurrentContactDate"`
	Direction                   int32  `json:"Direction"`
	ChannelID                   string `json:"ChannelID"`
	StateIndex                  int32  `json:"StateIndex"`
	CaseIDString                string `json:"CaseIDString"`
	DigitalContactState         int32  `json:"DigitalContactState"`
	PreviousQueueID             string `json:"PreviousQueueID,omitempty"`
	PreviousAgentUserID         string `json:"PreviousAgentUserID,omitempty"`
	PreviousContactState        int32  `json:"PreviousContactState,omitempty"`
	PreviousContactDate         string `json:"PreviousContactDate,omitempty"`
	PreviousDigitalContactState int32  `json:"PreviousDigitalContactState,omitempty"`
	EventID                     string `json:"EventID"`
}
