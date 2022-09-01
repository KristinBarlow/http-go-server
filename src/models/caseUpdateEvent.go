package models

type CaseUpdateEventWrapper struct {
	Updates []CaseUpdateEvent `json:"updates"`
}

type CaseUpdateEvent struct {
	EventID   string        `json:"eventID"`
	CreatedAt CreatedAtUnix `json:"createdAt"`
	Type      int32         `json:"type"`
	Brand     BrandView     `json:"brand"`
	Channel   ChannelView   `json:"channel"`
	Case      CaseView      `json:"case"`
}

type CreatedAtUnix struct {
	Seconds int32 `json:"seconds"`
	Nanos   int32 `json:"nanos"`
}

type BrandView struct {
	BrandID        int32  `json:"ID"`
	TenantID       string `json:"tenantID"`
	BusinessUnitID int64  `json:"businessUnitID"`
}

type ChannelView struct {
	ChannelD                        string `json:"ID"`
	Name                            string `json:"name"`
	ChannelIntegrationBoxIdentifier string `json:"integrationBoxIdentifier"`
	ChannelIdOnExternalPlatform     string `json:"idOnExternalPlatform"`
	ChannelRealExternalPlatformID   string `json:"realExternalPlatformID"`
	ChannelIsPrivate                bool   `json:"isPrivate"`
}

type CaseView struct {
	CaseID                int64                       `json:"ID"`
	ThreadID              string                      `json:"postID"`
	InteractionID         string                      `json:"interactionID"`
	Status                string                      `json:"status"`
	RoutingQueueID        string                      `json:"routingQueueID"`
	CaseInboxAssignee     int32                       `json:"inboxAssignee"`
	CaseOwnerAssignee     int32                       `json:"ownerAssignee"`
	CaseEndUserRecipients []CaseEndUserRecipientsView `json:"endUserRecipients"`
	DetailUrl             string                      `json:"detailUrl"`
	ContactGuid           string                      `json:"contactGuid"`
	CustomerContactID     string                      `json:"customerContactID"`
	UserInfo              CaseUserInfoView            `json:"UserInfo"`
}

type CaseEndUserRecipientsView struct {
	EndUserIdOnExternalPlatform string `json:"idOnExternalPlatform"`
	EndUserName                 string `json:"name"`
	EndUserIsPrimary            bool   `json:"isPrimary"`
}

type CaseUserInfoView struct {
	CaseAuthorEndUserIdentity AuthorEndUserIdentityView `json:"AuthorEndUserIdentity"`
}

type AuthorEndUserIdentityView struct {
	AuthorEndUserID                   string `json:"ID"`
	AuthorEndUserIdOnExternalPlatform string `json:"idOnExternalPlatform"`
	AuthorEndUserFullName             string `json:"fullName"`
}

func (c CaseUpdateEventWrapper) CaseUpdates() []CaseUpdateEvent {
	return c.Updates
}

func NewCaseUpdates(updates []CaseUpdateEvent) CaseUpdateEventWrapper {
	return CaseUpdateEventWrapper{
		Updates: updates,
	}
}
