package models

type DfoContactSearchResponse struct {
	Hits        int32      `json:"hits"`
	Data        []DataView `json:"data"`
	ScrollToken string     `json:"scrollToken,omitempty"`
}

type DataView struct {
	Id                          string                        `json:"id"`
	ThreadId                    string                        `json:"threadId"`
	ThreadIdOnExternalPlatform  string                        `json:"threadIdOnExternalPlatform,omitempty"`
	ChannelId                   string                        `json:"channelId"`
	InteractionId               string                        `json:"interactionId,omitempty"`
	ConsumerContactStorageId    string                        `json:"consumerContactStorageId,omitempty"`
	ContactGuid                 string                        `json:"contactId,omitempty"`
	CustomerContactId           string                        `json:"customerContactId,omitempty"`
	Status                      string                        `json:"status"`
	StatusUpdatedAt             string                        `json:"statusUpdatedAt"`
	RoutingQueueId              string                        `json:"routingQueueId"`
	RoutingQueuePriority        int32                         `json:"routingQueuePriority"`
	InboxAssignee               int64                         `json:"inboxAssignee,omitempty"`
	InboxAssigneeLastAssignedAt string                        `json:"inboxAssigneeLastAssignedAt,omitempty"`
	InboxAssigneeUser           DataInboxAssigneeUserView     `json:"inboxAssigneeUser,omitempty"`
	OwnerAssigneeUser           DataOwnerAssigneeUserView     `json:"ownerAssigneeUser,omitempty"`
	InboxPreAssigneeUser        DataInboxPreAssigneeUserView  `json:"inboxPreAssigneeUser,omitempty"`
	OwnerAssignee               int64                         `json:"ownerAssignee,omitempty"`
	EndUserRecipients           []Recipient                   `json:"endUserRecipients,omitempty"`
	Recipients                  []DataRecipientsView          `json:"recipients,omitempty"`
	RecipientsCustomers         []DataRecipientsCustomersView `json:"recipientsCustomers,omitempty"`
	DetailUrl                   string                        `json:"detailUrl"`
	AuthorEndUserIdentity       DataAuthorEndUserIdentityView `json:"authorEndUserIdentity,omitempty"`
	Direction                   string                        `json:"direction"`
	CreatedAt                   *CustomTimestamp              `json:"createdAt"`
	InboxPreAssignee            int32                         `json:"inboxPreAssignee,omitempty"`
	CustomFields                []DataCustomFieldsView        `json:"customerFields,omitempty"`
	UserFingerprint             DataUserFingerprintView       `json:"userFingerprint,omitempty"`
	Statistics                  DataStatisticsView            `json:"statistics,omitempty"`
	EndUser                     DataEndUserView               `json:"endUser,omitempty"`
	TenantID                    string
	Err                         error
	UpdateObj                   string
}

type DataInboxAssigneeUserView struct {
	Id            int32  `json:"id,omitempty"`
	IncontactId   string `json:"incontactId,omitempty"`
	EmailAddress  string `json:"emailAddress,omitempty"`
	LoginUsername string `json:"loginUsername,omitempty"`
	FirstName     string `json:"firstName,omitempty"`
	Surname       string `json:"surname,omitempty"`
	Nickname      string `json:"nickname,omitempty"`
	ImageUrl      string `json:"imageUrl,omitempty"`
	IsBotUser     bool   `json:"isBotUser,omitempty"`
	IsSurveyUser  bool   `json:"isSurveyUser,omitempty"`
}

type DataOwnerAssigneeUserView struct {
	Id             int32  `json:"id,omitempty"`
	IncontactId    string `json:"incontactId,omitempty"`
	EmailAddress   string `json:"emailaddress,omitempty"`
	LoginUsername  string `json:"loginUsername,omitempty"`
	FirstName      string `json:"firstName,omitempty"`
	Surname        string `json:"surname,omitempty"`
	Nickname       string `json:"nickname,omitempty"`
	ImageUrl       string `json:"imageUrl,omitempty"`
	PublidImageUrl string `json:"publicImageUrl"`
	IsBotUser      bool   `json:"isBotUser,omitempty"`
	IsSurveyUser   bool   `json:"isSurveyUser,omitempty"`
}

type DataInboxPreAssigneeUserView struct {
	Id            int32  `json:"id,omitempty"`
	IncontactId   string `json:"incontactId,omitempty"`
	EmailAddress  string `json:"emailAddress,omitempty"`
	LoginUsername string `json:"loginUsername,omitempty"`
	FirstName     string `json:"firstName,omitempty"`
	Surname       string `json:"surname,omitempty"`
	Nickname      string `json:"nickname,omitempty"`
	ImageUrl      string `json:"imageUrl,omitempty"`
	IsBotUser     bool   `json:"isBotUser,omitempty"`
	IsSurveyUser  bool   `json:"isSurveyUser,omitempty"`
}

type Recipients struct {
	EndUserIdOnExternalPlatform string `json:"idOnExternalPlatform,omitempty"`
	EndUserName                 string `json:"name,omitempty"`
	EndUserIsPrimary            bool   `json:"isPrimary,omitempty"`
	EndUserIsPrivate            bool   `json:"isPrivate,omitempty"`
}

type DataRecipientsView struct {
	RecipientsIdOnExternalPlatform string `json:"idOnExternalPlatform,omitempty"`
	RecipientsName                 string `json:"name,omitempty"`
	RecipientsIsPrimary            bool   `json:"isPrimary,omitempty"`
	RecipientsIsPrivate            bool   `json:"isPrivate,omitempty"`
}

type DataRecipientsCustomersView struct {
	Id           string                       `json:"id,omitempty"`
	FirstName    string                       `json:"firstName,omitempty"`
	Surname      string                       `json:"surname,omitempty"`
	FullName     string                       `json:"fullName,omitempty"`
	CustomFields []RecipientsCustomFieldsView `json:"customFields,omitempty"`
	Image        string                       `json:"image,omitempty"`
}

type DataAuthorEndUserIdentityView struct {
	IdOnExternalPlatform string `json:"idOnExternalPlatform"`
	FirstName            string `json:"firstName"`
	LastName             string `json:"lastName"`
	Nickname             string `json:"nickname"`
	Image                string `json:"image"`
	ExternalPlatformId   string `json:"externalPlatformId"`
	Id                   string `json:"id"`
	FullName             string `json:"fullName"`
}

type DataCustomFieldsView struct {
	Ident     string `json:"ident,omitempty"`
	Value     string `json:"value,omitempty"`
	UpdatedAt string `json:"updatedAt,omitempty"`
}

type RecipientsCustomFieldsView struct {
	Ident     string `json:"ident,omitempty"`
	Value     string `json:"value,omitempty"`
	UpdatedAt string `json:"updatedAt,omitempty"`
}

type DataUserFingerprintView struct {
	Browser         string `json:"browser,omitempty"`
	BrowserVersion  string `json:"browserVersion,omitempty"`
	Os              string `json:"os,omitempty"`
	OsVersion       string `json:"osVersion,omitempty"`
	Language        string `json:"language,omitempty"`
	Ip              string `json:"ip,omitempty"`
	Location        string `json:"location,omitempty"`
	Country         string `json:"country,omitempty"`
	DeviceType      string `json:"deviceType,omitempty"`
	DeviceToken     string `json:"deviceToken,omitempty"`
	ApplicationType string `json:"applicationType,omitempty"`
}

type DataStatisticsView struct {
	InboxAssigneeResponseTime InboxAssigneeResponseTimeView `json:"inboxAssigneeResponseTime,omitempty"`
}

type InboxAssigneeResponseTimeView struct {
	inboxAssigneeResponseTimevalueInSeconds int32 `json:"inboxAssigneeResponseTimevalueInSeconds,omitempty"`
	SlaEnabled                              bool  `json:"slaEnabled,omitempty"`
	SlaInSeconds                            int32 `json:"slaInSeconds,omitempty"`
	IsRunning                               bool  `json:"isRunning,omitempty"`
}

type DataEndUserView struct {
	Id           string                    `json:"id,omitempty"`
	FirstName    string                    `json:"firstName,omitempty"`
	Surname      string                    `json:"surname"`
	FullName     string                    `json:"fullName"`
	CustomFields []EndUserCustomFieldsView `json:"customFields,omitempty"`
	Image        string                    `json:"image"`
	Identities   []DataIdentitiesView      `json:"identities,omitempty"`
}

type EndUserCustomFieldsView struct {
	Ident     string `json:"ident,omitempty"`
	Value     string `json:"value,omitempty"`
	UpdatedAt string `json:"updatedAt,omitempty"`
}

type DataIdentitiesView struct {
	EndUserIdOnExternalPlatform string `json:"idOnExternalPlatform,omitempty"`
	EndUserFirstName            string `json:"firstName,omitempty"`
	EndUserLastName             string `json:"lastName,omitempty"`
	EndUserNickname             string `json:"nickname,omitempty"`
	EndUserImage                string `json:"image,omitempty"`
	EndUserExternalPlatformId   string `json:"externalPlatformId,omitempty"`
	EndUserId                   string `json:"id,omitempty"`
}

func (d DfoContactSearchResponse) ResponseHits() int32 {
	return d.Hits
}

func (d DfoContactSearchResponse) ResponseData() []DataView {
	return d.Data
}

func (d DfoContactSearchResponse) ResponseScrollToken() string {
	return d.ScrollToken
}

func NewDfoData(hits int32, data []DataView, scrollToken string) DfoContactSearchResponse {
	return DfoContactSearchResponse{
		Hits:        hits,
		Data:        data,
		ScrollToken: scrollToken,
	}
}
