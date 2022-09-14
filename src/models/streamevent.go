package models

import (
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type EventObject int

// EventObjectFromString converts a string into an EventObject.
// If e is not a valid EventObject then EventObject 0 will be returned
func EventObjectFromString(e string) EventObject {
	return eventObject_value[e]
}

func (e EventObject) String() string {
	return eventObject_name[e]
}

func (e EventObject) MarshalJSON() ([]byte, error) {
	s := e.String()
	b, err := json.Marshal(s)
	return b, err
}

func (e *EventObject) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	if err != nil {
		*e = EventObject_Undefined
		return nil
	}
	*e = EventObjectFromString(s)
	return nil
}

// NumEventObjects is primarily useful in automated testing to
// determine the total number of possible EventObject values
func NumEventObjects() int {
	return int(lastEventObject)
}

// Acceptable `EventObject` values
const (
	// enum value 0 is used internally to determine if the user set an EventObject
	EventObject_Undefined EventObject = iota
	EventObject_Channel
	EventObject_RoutingQueue
	EventObject_Case
	EventObject_Message
	EventObject_Thread
	EventObject_Contact // Sent on the platform stream but the eventObject for the customerContact events is Contact
	lastEventObject     // this EventType should never be used in code and should always remain as the last element in this iota block
)

var eventObject_name = map[EventObject]string{
	EventObject_Undefined:    "Undefined",
	EventObject_Channel:      "Channel",
	EventObject_RoutingQueue: "RoutingQueue",
	EventObject_Case:         "Case",
	EventObject_Message:      "Message",
	EventObject_Thread:       "Thread",
	EventObject_Contact:      "Contact",
}

var eventObject_value = map[string]EventObject{
	"EventObject_Undefined": EventObject_Undefined,
	"Channel":               EventObject_Channel,
	"RoutingQueue":          EventObject_RoutingQueue,
	"Case":                  EventObject_Case,
	"Message":               EventObject_Message,
	"Thread":                EventObject_Thread,
	"Contact":               EventObject_Contact,
}

type EventType int

// EventTypeFromString converts a string into an EventType.
// If e is not a valid EventType then EventType 0 will be returned
func EventTypeFromString(e string) EventType {
	return eventType_value[e]
}

func (e EventType) String() string {
	return eventType_name[e]
}

func (e EventType) MarshalJSON() ([]byte, error) {
	s := e.String()
	b, err := json.Marshal(s)
	return b, err
}

func (e *EventType) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	if err != nil {
		*e = EventType_Undefined
		return nil
	}
	*e = EventTypeFromString(s)
	return nil
}

// NumEventObjects is primarily useful in automated testing to
// determine the total number of possible EventObject values
func NumEventTypes() int {
	return int(lastEventType)
}

// Acceptable `EventType` values
const (
	// enum value 0 is used internally to determine if the user set an EventType
	EventType_Undefined EventType = iota
	EventType_ChannelCreated
	EventType_ChannelDeleted
	EventType_ChannelUpdated
	EventType_RoutingQueueCreated
	EventType_RoutingQueueDeleted
	EventType_RoutingQueueUpdated
	EventType_UserAssignedToRoutingQueue
	EventType_UserUnassignedFromRoutingQueue
	EventType_CaseCreated
	EventType_CaseStatusChanged
	EventType_CaseToRoutingQueueChanged
	EventType_CaseInboxAssigneeChanged
	EventType_CaseMessageAdded
	EventType_CaseAgentStarted
	EventType_CaseAgentEnded
	EventType_MessageCreated
	EventType_MessageUpdated
	EventType_MessageReadChanged
	EventType_MessageSeenByUser
	EventType_MessageSeenByEndUser
	EventType_MessageDeliveredToEndUser
	EventType_MessageDeliveredToUser
	EventType_ThreadFocused
	EventType_ThreadUnfocused
	EventType_CustomerContactClosed
	EventType_CustomerContactCreated
	lastEventType // this EventType should never be used in code and should always remain as the last element in this iota block
)

var eventType_name = map[EventType]string{
	EventType_Undefined:                      "Undefined",
	EventType_ChannelCreated:                 "ChannelCreated",
	EventType_ChannelDeleted:                 "ChannelDeleted",
	EventType_ChannelUpdated:                 "ChannelUpdated",
	EventType_RoutingQueueCreated:            "RoutingQueueCreated",
	EventType_RoutingQueueDeleted:            "RoutingQueueDeleted",
	EventType_RoutingQueueUpdated:            "RoutingQueueUpdated",
	EventType_UserAssignedToRoutingQueue:     "UserAssignedToRoutingQueue",
	EventType_UserUnassignedFromRoutingQueue: "UserUnassignedFromRoutingQueue",
	EventType_CaseCreated:                    "CaseCreated",
	EventType_CaseStatusChanged:              "CaseStatusChanged",
	EventType_CaseToRoutingQueueChanged:      "CaseToRoutingQueueAssignmentChanged",
	EventType_CaseInboxAssigneeChanged:       "CaseInboxAssigneeChanged",
	EventType_CaseMessageAdded:               "MessageAddedIntoCase",
	EventType_CaseAgentStarted:               "AgentContactStarted",
	EventType_CaseAgentEnded:                 "AgentContactEnded",
	EventType_MessageCreated:                 "MessageCreated",
	EventType_MessageUpdated:                 "MessageUpdated",
	EventType_MessageReadChanged:             "MessageReadChanged",
	EventType_MessageSeenByUser:              "MessageSeenByUser",
	EventType_MessageSeenByEndUser:           "MessageSeenByEndUser",
	EventType_MessageDeliveredToEndUser:      "MessageDeliveredToEndUser",
	EventType_MessageDeliveredToUser:         "MessageDeliveredToUser",
	EventType_ThreadFocused:                  "ThreadFocused",
	EventType_ThreadUnfocused:                "ThreadUnfocused",
	EventType_CustomerContactClosed:          "CustomerContactClosed",
	EventType_CustomerContactCreated:         "CustomerContactCreated",
}

var eventType_value = map[string]EventType{
	"EventType_Undefined":                 EventType_Undefined,
	"ChannelCreated":                      EventType_ChannelCreated,
	"ChannelDeleted":                      EventType_ChannelDeleted,
	"ChannelUpdated":                      EventType_ChannelUpdated,
	"RoutingQueueCreated":                 EventType_RoutingQueueCreated,
	"RoutingQueueDeleted":                 EventType_RoutingQueueDeleted,
	"RoutingQueueUpdated":                 EventType_RoutingQueueUpdated,
	"UserAssignedToRoutingQueue":          EventType_UserAssignedToRoutingQueue,
	"UserUnassignedFromRoutingQueue":      EventType_UserUnassignedFromRoutingQueue,
	"CaseCreated":                         EventType_CaseCreated,
	"CaseStatusChanged":                   EventType_CaseStatusChanged,
	"CaseToRoutingQueueAssignmentChanged": EventType_CaseToRoutingQueueChanged,
	"CaseInboxAssigneeChanged":            EventType_CaseInboxAssigneeChanged,
	"MessageAddedIntoCase":                EventType_CaseMessageAdded,
	"AgentContactStarted":                 EventType_CaseAgentStarted,
	"AgentContactEnded":                   EventType_CaseAgentEnded,
	"MessageCreated":                      EventType_MessageCreated,
	"MessageUpdated":                      EventType_MessageUpdated,
	"MessageReadChanged":                  EventType_MessageReadChanged,
	"MessageSeenByUser":                   EventType_MessageSeenByUser,
	"MessageSeenByEndUser":                EventType_MessageSeenByEndUser,
	"MessageDeliveredToEndUser":           EventType_MessageDeliveredToEndUser,
	"MessageDeliveredToUser":              EventType_MessageDeliveredToUser,
	"ThreadFocused":                       EventType_ThreadFocused,
	"ThreadUnfocused":                     EventType_ThreadUnfocused,
	"CustomerContactClosed":               EventType_CustomerContactClosed,
	"CustomerContactCreated":              EventType_CustomerContactCreated,
}

// `FieldName` values for items with an `EventObject` value of `Channel` and `EventType` value of `ChannelUpdate`
const (
	ChannelUpdateChangesFieldName_name                    = "name"
	ChannelUpdatedChangesFieldName_idOnExternalPlatform   = "idOnExternalPlatform"
	ChannelUpdatedChangesFieldName_RealExternalPlatformId = "realExternalPlatformId"
)

// `FieldName` values for items with an `EventObject` value of `RoutingQueue` and `EventType` value of `RoutingQueueUpdated`
const (
	RoutingQueueUpdatedChangesFieldName_name = "name"
)

// CustomTimestamp is used to override json unmarshalling of incoming timestamps and
// coerce them into a timestamppb.Timestamp data type
type CustomTimestamp timestamppb.Timestamp

// Timestamp converts c into a Timestamp
func (c *CustomTimestamp) Timestamp() *timestamppb.Timestamp {
	if c == nil {
		return nil
	}
	return (*timestamppb.Timestamp)(c)
}

func (c *CustomTimestamp) MarshalJSON() ([]byte, error) {
	var s string

	if c.Nanos > 0 {
		s = time.Unix(c.Seconds, int64(c.Nanos)).UTC().Format(time.RFC3339Nano)
	} else {
		s = time.Unix(c.Seconds, int64(c.Nanos)).UTC().Format(time.RFC3339)
	}
	return json.Marshal(s)
}

func (c *CustomTimestamp) UnmarshalJSON(data []byte) error {
	// Data could be either a string or a struct.
	var attempt1 string
	err := json.Unmarshal(data, &attempt1)
	if err == nil && attempt1 != "" {
		parsedTime, err := time.Parse(time.RFC3339, attempt1)
		if err == nil {
			c.Nanos = int32(parsedTime.Nanosecond())
			c.Seconds = parsedTime.Unix()
			return nil
		}
		return fmt.Errorf("failed to unmarshal CustomTimestamp string: %w", err)
	}

	err = json.Unmarshal(data, (*timestamppb.Timestamp)(c))
	if err == nil {
		return nil
	}

	return fmt.Errorf("failed to unmarshal CustomTimestamp struct: %w", err)
}

// Stream event json must match incoming values from DFO (see https://tlvconfluence01.nice.com/pages/viewpage.action?pageId=710149026)
type AgentContact struct {
	ID        string           `json:"id"`
	User      User             `json:"user"`
	CreatedAt *CustomTimestamp `json:"createdAt"`
	ClosedAt  *CustomTimestamp `json:"closedAt"`
}

type Brand struct {
	ID             int64  `json:"id"`
	TenantID       string `json:"tenantId"`
	BusinessUnitID int32  `json:"businessUnitId"`
}

type Case struct {
	ID                    string              `json:"id"`
	ThreadId              string              `json:"threadId"`
	InteractionId         string              `json:"interactionId"`
	Status                string              `json:"status"`
	RoutingQueueId        string              `json:"routingQueueId"`
	RoutingQueuePriority  int32               `json:"routingQueuePriority"`
	InboxAssignee         int64               `json:"inboxAssignee"`
	OwnerAssignee         int64               `json:"ownerAssignee"`
	EndUserRecipients     []Recipient         `json:"endUserRecipients,omitempty"`
	RecipientsCustomers   []RecipientCustomer `json:"recipientsCustomers"`
	Direction             string              `json:"direction"`
	AuthorEndUserIdentity EndUserIdentity     `json:"authorEndUserIdentity"`
	AuthorUser            User                `json:"authorUser"`
	DetailUrl             string              `json:"detailUrl"`
	ContactId             string              `json:"contactId"` // Becomes ContactGuid
	CustomerContactId     string              `json:"customerContactId"`
}

type Changes struct {
	FieldName    string `json:"fieldName"`
	CurrentValue string `json:"currentValue"`
}

type Channel struct {
	ID                       string    `json:"id"`
	Name                     string    `json:"name"`
	IDOnExternalPlatform     string    `json:"idOnExternalPlatform"`
	IsPrivate                bool      `json:"isPrivate"`
	RealExternalPlatformID   string    `json:"realExternalPlatformId"`
	StudioScript             string    `json:"studioScript,omitempty"`
	IntegrationBoxIdentifier string    `json:"integrationBoxIdentifier"`
	Changes                  []Changes `json:"_changes,omitempty"`
}

type ContentRemoved struct {
	Reason    string           `json:"reason"`
	RemovedAt *CustomTimestamp `json:"removedAt"`
}

type CustomerContact struct {
	ID        string           `json:"id"`
	CreatedAt *CustomTimestamp `json:"createdAt"`
	ClosedAt  *CustomTimestamp `json:"closedAt"`
}

type Data struct {
	Brand           Brand           `json:"brand"`
	Channel         Channel         `json:"channel"`
	CustomerContact CustomerContact `json:"customerContact"`
	RoutingQueue    RoutingQueue    `json:"routingQueue"`
	SubQueue        SubQueue        `json:"subqueue"`
	User            User            `json:"user"`
	InboxAssignee   User            `json:"inboxAssignee"`
	Interaction     Interaction     `json:"interaction"`
	Case            Case            `json:"case"`
	Contact         Case            `json:"contact,omitempty"` // Always the same obj behind the scenes as Case - DFO renamed the case view to contact view for new streams - Needed for JSON parsing
	AgentContact    AgentContact    `json:"agentContact"`
	Thread          Thread          `json:"thread,omitempty"`
	Message         Message         `json:"message"`
}

type EndUser struct {
	ID        string `json:"id"`
	FirstName string `json:"firstName"`
	Surname   string `json:"surname"`
}

type EndUserIdentity struct {
	FirstName            string `json:"firstName"`
	FullName             string `json:"fullName"`
	ID                   string `json:"ID"`
	IdOnExternalPlatform string `json:"idOnExternalPlatform"`
	Image                string `json:"image"`
	LastName             string `json:"lastName"`
	NickName             string `json:"nickname"`
}

type Interaction struct {
	ID        string           `json:"id"`
	CreatedAt *CustomTimestamp `json:"createdAt"`
	ClosedAt  *CustomTimestamp `json:"closedAt"`
}

type Message struct {
	ID                         string             `json:"ID"`
	AuthorEndUserIdentity      EndUserIdentity    `json:"authorEndUserIdentity"`
	AuthorNameRemoved          ContentRemoved     `json:"authorNameRemoved"`
	AuthorUser                 User               `json:"authorUser"`
	ContentRemoved             ContentRemoved     `json:"contentRemoved"`
	CreatedAt                  *CustomTimestamp   `json:"createdAt"`
	DeletedOnExternalPlatform  bool               `json:"deletedOnExternalPlatform"`
	Direction                  string             `json:"direction"`
	IdOnExternalPlatform       string             `json:"idOnExternalPlatform"`
	IsHiddenOnExternalPlatform bool               `json:"isHiddenOnExternalPlatform"`
	IsRead                     bool               `json:"isRead"`
	MessageContent             MessageContent     `json:"messageContent"`
	ReactionStatistics         ReactionStatistics `json:"reactionStatistics"`
	ReadAt                     *CustomTimestamp   `json:"readAt"`
	ReplyToMessage             ReplyToMessage     `json:"replyToMessage"`
	Sentiment                  string             `json:"sentiment"`
	Tags                       []Tag              `json:"tags"`
	ThreadId                   string             `json:"threadId"`
}

type MessageContent struct {
	Text    string  `json:"text"`
	Type    string  `json:"type"`
	Payload Payload `json:"payload"`
}

type Payload struct {
	Text     string `json:"text"`
	Postback string `json:"postback"`
}

type ReactionStatistics struct {
	IsLikedByChannel  bool `json:"isLikedByChannel"`
	IsSharedByChannel bool `json:"isSharedByChannel"`
	Likes             int  `json:"likes"`
	Shares            int  `json:"shares"`
}

type Recipient struct {
	IdOnExternalPlatform string `json:"idOnExternalPlatform"`
	Name                 string `json:"name"`
	IsPrimary            bool   `json:"isPrimary"`
	IsPrivate            bool   `json:"isPrivate"`
}

type RecipientCustomer struct {
	Id        string `json:"id"`
	FirstName string `json:"firstName"`
	Surname   string `json:"surname"`
	FullName  string `json:"fullName"`
}

type ReplyToMessage struct {
	ID                   string `json:"ID"`
	IdOnExternalPlatform string `json:"idOnExternalPlatform"`
}

type RoutingQueue struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	IsSubQueue bool      `json:"isSubqueue"`
	Changes    []Changes `json:"_changes,omitempty"`
}

type StreamEventRequest struct {
	EventID                   string           `json:"eventId"`
	EventObject               EventObject      `json:"eventObject"`
	EventType                 EventType        `json:"eventType"`
	CreatedAt                 *CustomTimestamp `json:"createdAt"`
	CreatedAtWithMilliseconds *CustomTimestamp `json:"createdAtWithMilliseconds"`
	Data                      Data             `json:"data"`
}

type SubQueue struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	IsSubQueue bool   `json:"isSubqueue"`
}

type Tag struct {
	ID    int    `json:"id"`
	Color string `json:"color"`
	Title string `json:"title"`
}

type Thread struct {
	ID                   string `json:"id,omitempty"`
	IdOnExternalPlatform string `json:"idOnExternalPlatform,omitempty"`
	ThreadName           string `json:"threadName,omitempty"`
}

type User struct {
	ID            int64  `json:"id"`
	InContactID   string `json:"incontactId"`
	IsBotUser     bool   `json:"isBotUser"`
	EmailAddress  string `json:"emailAddress"`
	LoginUsername string `json:"loginUsername"`
	FirstName     string `json:"firstName"`
	SurName       string `json:"surname"`
	NickName      string `json:"nickname"`
	ImageUrl      string `json:"imageUrl"`
	IsSurveyUser  bool   `json:"isSurveyUser"`
}
