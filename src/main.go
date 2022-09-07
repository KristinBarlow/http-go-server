package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/MyGoProjects/http-go-server/src/models"
	"github.com/google/uuid"
	db "github.com/inContact/orch-common/dbmappings"
	"github.com/inContact/orch-digital-middleware/pkg/digiservice"
	"github.com/inContact/orch-digital-middleware/pkg/digitransport"
	"google.golang.org/grpc"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type TenantIdsObj struct {
	TenantIDs [1]string `json:"tenantIds"`
	State     [5]string `json:"state"`
}

type DmwKnownContacts struct {
	Contacts []DmwKnownContact `json:"contacts"`
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

type DfoAuthTokenBody struct {
	GrantType    string `json:"grant_type"`
	ClientId     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

type DfoAuthTokenObj struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int32  `json:"expires_in"`
	TokenType   string `json:"token_type"`
	Scope       string `json:"scope,omitempty"`
}

const (
	regionRequest       string = "region - \"na1\" (Oregon), \"au1\" (Australia), \"eu1\" (Frankfurt), \"jp1\" (Japan), \"uk1\" (London), \"ca1\" (Montreal)"
	envRequest          string = "environment - \"dev\", \"test\", \"staging\", \"prod\""
	tenantGuidRequest   string = "tenantID (in GUID format)"
	clientIdRequest     string = "clientId"
	clientSecretRequest string = "clientSecret"
)

const CONTACTENDED = "ContactEnded"
const CONTACTSTATEUPDATED = "ContactStateUpdated"
const DMWAPIURLPREFIX = "http://digi-shared-eks01-"
const DFOAPIURLPREFIX = "https://api-de-"

func main() {
	ctx := context.Background()
	region, env, tenantId, clientId, clientSecret := "", "", "", "", ""
	var tenants [1]string

	// Prompt for needed input data
	//region = PromptForInputData("region", regionRequest)
	//env = PromptForInputData("env", envRequest)
	//tenantId = PromptForInputData("tenantId", tenantGuidRequest)
	//clientId = PromptForInputData("clientCreds", clientIdRequest)
	//clientSecret = PromptForInputData("clientCreds", clientSecretRequest)

	region = "na1"
	env = "dev"
	tenantId = "11EB505F-7844-7680-923B-0242AC110003"
	clientId = "cNqAL5N8CyGT5oMTTczpdSeDQnxTN2hyeg4m4QAqjY2s5"
	clientSecret = "2ZLdiztGgAcPlHunzTX355M8Hx9fqHunNvXctE0iWYBru"

	// Build api call URIs
	dmwContactStateApiUrl := buildUri("dmwGetStates", DMWAPIURLPREFIX, region, env)
	dfoAuthTokenApiUrl := buildUri("dfoAuth", DFOAPIURLPREFIX, region, env)
	dfoContactSearchApiUrl := buildUri("dfoContacts", DFOAPIURLPREFIX, region, env)
	dfoContactByIdApiUrl := buildUri("dfoContactById", DFOAPIURLPREFIX, region, env)

	// Get list of Digimiddleware known active contacts
	tenants[0] = tenantId
	dmwKnownContacts, err := GetDmwActiveContactStateData(dmwContactStateApiUrl, tenants)
	if err != nil {
		fmt.Printf("Error calling DMW GET ContactStates API.  Err: %v", err)
	}

	// Get DFO auth token
	dfoAuthTokenObj, err := GetDfoAuthToken(dfoAuthTokenApiUrl, clientId, clientSecret)
	if err != nil {
		fmt.Printf("Error calling DFO 3.0 GET AuthToken API.  Err: %v", err)
	}

	// Get list of DFO active contacts
	dfoData, err := MakeDfoContactSearchApiCall(dfoContactSearchApiUrl, dfoAuthTokenObj)
	if err != nil {
		fmt.Printf("Error calling DFO 3.0 Contact Search API.  Err: %v", err)
	}

	// Compare lists to get the contacts that exist in DMW but are closed in DFO.
	deltaContacts := GetDeltaList(dmwKnownContacts, dfoData)

	// Call DFO 3.0 GET Contacts by contactId for each record in Delta list to obtain actual metadata for contact
	actualMetaData, err := MakeDfoContactByIdApiCall(dfoContactByIdApiUrl, dfoAuthTokenObj, deltaContacts)
	fmt.Println(actualMetaData)

	//TODO: Call api to update DMW contact states to set contact to closed.
	//for _, contact := range deltaContacts.Contacts {
	//	var eventType string
	//	var newState string
	//	//var trigger fsm.Trigger
	//	//var err error
	//	if contact.newState > 0 {
	//		eventType = CONTACTSTATEUPDATED
	//	} else {
	//		eventType = CONTACTENDED
	//	}
	//	contact = digiEventUpdateContact(contact.EventID, contact.newState, event.GetCreatedAt(), nil, nil, eventType, action, existingContact)
	//
	//}

	// Create gRPC client to pass CaseEventUpdate to digimiddlware
	middlewareEventService := createGrpcClient(ctx)

	if middlewareEventService == nil {
		return
	}

	//TODO: build caseUpdateEvent to pass in place of nil below
	middlewareEventService.CaseEventUpdate(ctx, nil)
	// TODO: pass middlewareEventService to what will be making the Grpc service call
}

func buildUri(apiType string, apiPrefix string, region string, env string) string {
	uri := ""
	switch apiType {
	case "dmwGetStates":
		uri = apiPrefix + region + ".omnichannel." + env + ".internal:8085/digimiddleware/getstatesbytenants"
	case "dfoAuth":
		switch env {
		case "prod":
			uri = apiPrefix + region + ".niceincontact.com/engager/2.0/token"
		case "dev", "test", "staging":
			uri = apiPrefix + region + "." + env + ".niceincontact.com/engager/2.0/token"
		default:
			break
		}
	case "dfoContacts":
		switch env {
		case "prod":
			uri = apiPrefix + region + ".niceincontact.com/dfo/3.0/contacts?status[]=new&status[]=resolved&status[]=escalated&status[]=pending&status[]=open"
		case "dev", "test", "staging":
			uri = apiPrefix + region + "." + env + ".niceincontact.com/dfo/3.0/contacts?status[]=new&status[]=resolved&status[]=escalated&status[]=pending&status[]=open"
		default:
			break
		}
	case "dfoContactById":
		switch env {
		case "prod":
			uri = apiPrefix + region + ".niceincontact.com/dfo/3.0/contacts/"
		case "dev", "test", "staging":
			uri = apiPrefix + region + "." + env + ".niceincontact.com/dfo/3.0/contacts/"
		default:
			break
		}
	default:
		break
	}
	return uri
}

// PromptForInputData sends request for input data and validates the values, then returns the response
func PromptForInputData(inputType string, requestType string) string {
	response := GetInputData(requestType)

	validInputData := false
	for !validInputData {
		validInputData = ValidateResponse(response, inputType, validInputData)
		if !validInputData {
			response = GetInputData(requestType)
		}
	}
	return response
}

// GetInputData requests user input and returns value
func GetInputData(inputType string) string {
	fmt.Printf("Input %v': ", inputType)
	reader := bufio.NewReader(os.Stdin)

	response, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}

	return strings.TrimSuffix(response, "\n")
}

// ValidateResponse verifies that the data input by the user was in a proper type or format
func ValidateResponse(inputValue string, inputType string, inputValueValid bool) bool {
	switch inputType {
	case "region":
		switch strings.ToLower(inputValue) {
		case "na1", "au1", "eu1", "jp1", "uk1", "ca1":
			inputValueValid = true
		default:
			fmt.Println("Input value is not a valid region name (\"na1\", \"au1\", \"eu1\", \"jp1\", \"uk1\", \"ca1\")")
			inputValueValid = false
		}
	case "env":
		switch strings.ToLower(inputValue) {
		case "dev", "test", "staging", "prod":
			inputValueValid = true
		default:
			fmt.Println("Input value is not a valid environment name (\"dev\", \"test\", \"staging\", \"prod\")")
			inputValueValid = false
		}
	case "tenantId":
		if _, err := uuid.Parse(inputValue); err != nil {
			fmt.Printf("Input value is not a valid GUID: [%v]\n", inputValue)
			inputValueValid = false
		} else {
			inputValueValid = true
		}
	case "clientCreds":
		if len(inputValue) != 45 {
			fmt.Printf("Input value is not the proper string length: [%v]\n", inputValue)
			inputValueValid = false
		} else {
			inputValueValid = true
		}
	default:
		return inputValueValid
	}
	return inputValueValid
}

// GetDmwActiveContactStateData calls the Digimiddleware api POST digimiddleware/getstatesbytenants to get the list of contacts stored in DynamoDB
func GetDmwActiveContactStateData(apiUrl string, tenants [1]string) (DmwKnownContacts, error) {
	var dmwKnownContacts DmwKnownContacts
	contentType := "application/json"
	var tenantIdsObj TenantIdsObj
	tenantIdsObj.TenantIDs = tenants
	tenantIdsObj.State[0] = "new"
	tenantIdsObj.State[1] = "resolved"
	tenantIdsObj.State[2] = "pending"
	tenantIdsObj.State[3] = "escalated"
	tenantIdsObj.State[4] = "open"

	bodyJson, _ := json.Marshal(tenantIdsObj)
	reader := bytes.NewReader(bodyJson)
	var responseData []byte

	response, err := http.Post(apiUrl, contentType, reader)

	if err != nil {
		fmt.Print(err.Error())
		os.Exit(1)
		return dmwKnownContacts, err
	}

	if response.StatusCode == 200 {
		responseData, err = ioutil.ReadAll(response.Body)
		if err != nil {
			log.Fatal(err)
			return dmwKnownContacts, err
		}
	}

	if responseData != nil {
		err := json.Unmarshal(responseData, &dmwKnownContacts)
		if err != nil {
			fmt.Printf("Cannot unmarshal dmwResponse.  Error: %v", err)
			return dmwKnownContacts, err
		}
	}

	sort.SliceStable(dmwKnownContacts.Contacts, func(i, j int) bool {
		return dmwKnownContacts.Contacts[i].ContactID < dmwKnownContacts.Contacts[j].ContactID
	})

	return dmwKnownContacts, err
}

// GetDfoAuthToken calls the DFO api POST engager/2.0/token to get a bearer token for subsequent DFO api calls
func GetDfoAuthToken(apiUrl string, clientId string, clientSecret string) (DfoAuthTokenObj, error) {
	contentType := "application/json"
	var dfoAuthTokenBody DfoAuthTokenBody
	dfoAuthTokenBody.GrantType = "client_credentials"
	dfoAuthTokenBody.ClientId = clientId
	dfoAuthTokenBody.ClientSecret = clientSecret
	var dfoAuthTokenObj DfoAuthTokenObj

	bodyJson, _ := json.Marshal(dfoAuthTokenBody)
	reader := bytes.NewReader(bodyJson)
	var responseData []byte

	response, err := http.Post(apiUrl, contentType, reader)

	if err != nil {
		fmt.Print(err.Error())
		os.Exit(1)
		return dfoAuthTokenObj, err
	}

	if response.StatusCode == 200 {
		responseData, err = ioutil.ReadAll(response.Body)
		if err != nil {
			log.Fatal(err)
			return dfoAuthTokenObj, err
		}
	}

	if responseData != nil {
		err := json.Unmarshal(responseData, &dfoAuthTokenObj)
		if err != nil {
			fmt.Printf("Cannot unmarshal dfoAuthToken. Error: %v", err)
			return dfoAuthTokenObj, err
		}
	} else {
		fmt.Println("DfoAuthToken was null or empty.")
		return dfoAuthTokenObj, err
	}

	return dfoAuthTokenObj, err
}

// MakeDfoContactSearchApiCall calls the DFO 3.0 GET Contact Search Api then loops until all data is retrieved (api only returns 25 records at a time)
func MakeDfoContactSearchApiCall(dfoContactSearchApiUrl string, dfoAuthTokenObj DfoAuthTokenObj) ([]models.DataView, error) {
	var dfoActiveContactList models.DfoContactSearchResponse
	var dfoData []models.DataView
	var err error
	var hits int32 = 25

	// Call DFO 3.0 GET Contact Search which returns 1st 25 records
	dfoResponse := GetDfoActiveContactList(dfoContactSearchApiUrl, dfoAuthTokenObj)
	if dfoResponse != nil || len(dfoResponse) > 0 {
		err := json.Unmarshal(dfoResponse, &dfoActiveContactList)
		if err != nil {
			fmt.Printf("Cannot unmarshal dfoResponse to full object.  Error: %v", err)
			return dfoData, err
		}
	} else {
		fmt.Println("GetDfoActiveContactList returned 0 records")
		return dfoData, err
	}

	// Append first 25 Data records to Data list
	dfoData = append(dfoData, dfoActiveContactList.Data...)

	if dfoActiveContactList.Hits > 25 {
		// Sleep for 1 sec between calls to not overload the GET Contact Search api
		time.Sleep(1000 * time.Millisecond)
		for hits <= dfoActiveContactList.Hits {
			dfoContactSearchApiUrlSt := dfoContactSearchApiUrl + "&scrollToken=" + dfoActiveContactList.ScrollToken
			hits += 25

			// Call DFO 3.0 GET Contact Search to get next 25 records
			dfoResponse = GetDfoActiveContactList(dfoContactSearchApiUrlSt, dfoAuthTokenObj)
			fmt.Printf("dfoResponse [%v]: ", hits)

			if dfoResponse != nil {
				err := json.Unmarshal(dfoResponse, &dfoActiveContactList)
				if err != nil {
					fmt.Printf("Cannot unmarshal dfoResponse to full object.  Error: %v", err)
					return dfoData, err
				}
			}

			// Append next 25 Data records to Data list
			dfoData = append(dfoData, dfoActiveContactList.Data...)
		}
	}

	sort.SliceStable(dfoData, func(i, j int) bool {
		return dfoData[i].Id < dfoData[j].Id
	})

	return dfoData, err
}

// GetDfoActiveContactList calls DFO 3.0 api GET Contact Search which returns a list of active contacts for tenant auth token provided
func GetDfoActiveContactList(apiUrl string, dfoAuthTokenObj DfoAuthTokenObj) []byte {
	var bearer = dfoAuthTokenObj.TokenType + " " + dfoAuthTokenObj.AccessToken
	var responseData []byte

	// Create a new request using http
	req, err := http.NewRequest("GET", apiUrl, nil)

	// add authorization header and content-type to the req
	req.Header.Add("Authorization", bearer)
	req.Header.Add("Content-Type", "application/json")

	// Send req using http Client
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println("Error on response.\n[ERROR] -", err)
	}
	defer resp.Body.Close()

	responseData, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Error while reading the response bytes:", err)
	}

	return responseData
}

// GetDeltaList loops through the known contacts from DMW and compares them to the active contacts in DFO and creates a list of contacts that need to be updated
func GetDeltaList(dmwKnownContacts DmwKnownContacts, dfoData []models.DataView) DmwKnownContacts {
	type DmwContacts []DmwKnownContact
	deltaArray := DmwContacts{}
	var deltaList DmwKnownContacts

	// Loop through DmwKnownContacts and check each contact data in DfoActiveContacts to see if we find a match
	for _, contact := range dmwKnownContacts.Contacts {
		found := false
		requiresUpdate := false
		var realContactState db.InDataContactState
		// Only compare contact with DFO data if contact is not closed (18)
		if contact.CurrentContactState != 18 {
			// Compare the data from DFO with the DMW data
			for _, d := range dfoData {
				dataId, _ := strconv.ParseInt(d.Id, 10, 64)
				// Check if there is a match for an active contact in DfoData
				if contact.ContactID == dataId {
					found = true
					// Determine what the correct current contact state should be based on the dfoData.
					realContactState = determineContactStateFromData(d.InboxAssigneeUser.IncontactId, d.RoutingQueueId, d.Status)
					// If contact requiresUpdate and states match, no need to update state.
					if contact.CurrentContactState == int32(realContactState) {
						break
					} else {
						requiresUpdate = true
					}
				}
			}

			// If no match in dfoData, then we need to update contact to closed
			if !found {
				requiresUpdate = true
			}
		}

		// If no match is requiresUpdate or state needs to be updated, add contact data to deltaList
		if requiresUpdate {
			//fmt.Printf("ContactID*[%v]*CurrentContactState*[%v]*RealContactState*[%v]*CurrentContactDate*[%v]*Found*[%v]*RequiresUpdate*[%v]\n", contact.ContactID, contact.CurrentContactState, realContactState, contact.CurrentContactDate, found, requiresUpdate)

			//contact.CurrentContactDate = time.Now().String() //TODO: do we have to find the real time it was closed?
			contact.EventID = uuid.NewString()

			if found {
				contact.CurrentContactState = int32(realContactState)
			} else {
				contact.CurrentContactState = int32(db.InDataContactState_EndContact)
			}

			// TODO: We may not need all of this, maybe just the contactIds to send back to DFO to get the latest status
			delta := DmwKnownContact{
				ContactID:                   contact.ContactID,
				MasterContactID:             contact.MasterContactID,
				TenantID:                    contact.TenantID,
				QueueID:                     contact.QueueID,
				StartDate:                   contact.StartDate,
				FromAddr:                    contact.FromAddr,
				CurrentContactState:         contact.CurrentContactState,
				CurrentContactDate:          contact.CurrentContactDate,
				Direction:                   contact.Direction,
				ChannelID:                   contact.ChannelID,
				StateIndex:                  contact.StateIndex,
				CaseIDString:                contact.CaseIDString,
				DigitalContactState:         contact.DigitalContactState,
				PreviousQueueID:             contact.PreviousQueueID, // TODO: do I need to figure out the true previous data for these?
				PreviousAgentUserID:         contact.PreviousAgentUserID,
				PreviousContactState:        contact.PreviousContactState,
				PreviousContactDate:         contact.PreviousContactDate,
				PreviousDigitalContactState: contact.PreviousDigitalContactState,
				EventID:                     contact.EventID,
			}

			deltaArray = append(deltaArray, delta)

			deltaList = DmwKnownContacts{
				Contacts: deltaArray,
			}
			fmt.Printf("ContactID*[%v]*CurrentContactState*[%v]*RealContactState*[%v]*CurrentContactDate*[%v]*Found*[%v]*RequiresUpdate*[%v]\n", delta.ContactID, delta.CurrentContactState, realContactState, delta.CurrentContactDate, found, requiresUpdate)

		}
	}

	return deltaList
}

// MakeDfoContactByIdApiCall loops through the delta list of contacts and returns the closedContact metadata
func MakeDfoContactByIdApiCall(dfoContactByIdApiUrl string, dfoAuthTokenObj DfoAuthTokenObj, deltaList DmwKnownContacts) ([]models.DataView, error) {
	var dfoClosedContactData models.DataView
	var dfoData []models.DataView
	var err error

	for _, contact := range deltaList.Contacts {
		// Sleep for 1 sec between calls to not overload the GET Contact Search api
		time.Sleep(1000 * time.Millisecond)
		dfoResponse := GetDfoContactById(dfoContactByIdApiUrl, dfoAuthTokenObj, strconv.Itoa(int(contact.ContactID)))
		if dfoResponse != nil || len(dfoResponse) > 0 {
			err := json.Unmarshal(dfoResponse, &dfoClosedContactData)
			if err != nil {
				fmt.Printf("Cannot unmarshal dfoResponse to full object.  Error: %v", err)
				return dfoData, err
			}
		} else {
			fmt.Println("MakeDfoContactByIdApiCall returned 0 records")
		}

		// Append next 25 Data records to Data list
		dfoData = append(dfoData, dfoClosedContactData)
	}

	return dfoData, err
}

// GetDfoContactById calls DFO 3.0 GET contacts with contactID and returns the response object
func GetDfoContactById(dfoContactByIdApiUrl string, dfoAuthTokenObj DfoAuthTokenObj, contactId string) []byte {
	var bearer = dfoAuthTokenObj.TokenType + " " + dfoAuthTokenObj.AccessToken
	apiUrl := dfoContactByIdApiUrl + contactId
	var responseData []byte

	// Create a new request using http
	req, err := http.NewRequest("GET", apiUrl, nil)

	// add authorization header and content-type to the req
	req.Header.Add("Authorization", bearer)
	req.Header.Add("Content-Type", "application/json")

	// Send req using http Client
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println("Error on response.\n[ERROR] -", err)
	}
	defer resp.Body.Close()

	responseData, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Error while reading the response bytes:", err)
	}

	return responseData
}

// determineContactStateFromData determines contactState from digital contact state and details
func determineContactStateFromData(agentUserID, queueID, digitalContactState string) db.InDataContactState {
	var contactState db.InDataContactState
	switch digitalContactState {
	case "closed", "trashed":
		contactState = db.InDataContactState_EndContact
	case "pending", "escalated", "resolved":
		if agentUserID != "" {
			contactState = db.InDataContactState_Active
		} else {
			contactState = db.InDataContactState_PostQueue
		}
	case "new", "open":
		if agentUserID != "" {
			contactState = db.InDataContactState_Active
		} else if queueID == "" {
			contactState = db.InDataContactState_PreQueue
		} else {
			contactState = db.InDataContactState_InQueue
		}
	default:
		contactState = db.InDataContactState_Undefined
	}
	return contactState
}

func createGrpcClient(ctx context.Context) digiservice.GrpcService {
	digimiddlewareGrpcAddr := "digi-shared-eks01-na1.omnichannel.dev.internal:9884"
	conn, err := grpc.Dial(digimiddlewareGrpcAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("initialization of digimiddleware gRPC client failed with err: %w", err)
		return nil
	} else {
		fmt.Println("gRPC client initialized")
	}

	middlewareEventService := digitransport.NewGRPCClient(conn, nil, nil)
	return middlewareEventService
}

//func getCaseUpdateEvent(data models.DataView) {
//	// TODO: Need to call API to get data for each contact to update so we can fill in values below
//
//	brandView := models.BrandView{
//		BrandID:        1000, //DMW doesn't require the brand so we can hard-code any value here
//		TenantID:       contact.TenantID,
//		BusinessUnitID: 999, //TODO: How are we going to get this!!!
//	}
//
//	channelView := models.ChannelView{
//		ChannelD:                        contact.ChannelID,
//		Name:                            "",    //TODO: Search for recipients array where idOnExternalPlatform = SUFFIX of channelId then take the name
//		ChannelIntegrationBoxIdentifier: "",    //TODO: take the first part of the channelId OR recipientsCustomers.identities.externalPlatformId OR authorEndUserIdentity.identities.externalPlatformId OR endUser.identities.externalPlatformId
//		ChannelIdOnExternalPlatform:     "",    //TODO: take the channelId suffix after chat_ OR for inbound, search recipients array where idOnExternalPlatform matches the suffix of channelID
//		ChannelRealExternalPlatformID:   "",    //TODO: take the first part of the channelId OR recipientsCustomers.identities.externalPlatformId OR authorEndUserIdentity.identities.externalPlatformId OR endUser.identities.externalPlatformId
//		ChannelIsPrivate:                false, //TODO: recipients.isPrivate (where the recipients.idOnExternalPlatform = the last part of the channelId
//	}
//
//	caseEndUserRecipients := models.CaseEndUserRecipientsView{
//		EndUserIdOnExternalPlatform: "",   //TODO: endUserRecipients.idOnExternalPlatform (if multiple, take one where isPrimary = true)
//		EndUserName:                 "",   //TODO: endUserRecipients.name (if multiple, take one where isPrimary = true)
//		EndUserIsPrimary:            true, //TODO: endUserRecipients.isPrimary
//	}
//
//	caseView := models.CaseView{
//		CaseID:                contact.ContactID,
//		ThreadID:              data.ThreadId,
//		InteractionID:         data.InteractionId,
//		Status:                data.Status,
//		RoutingQueueID:        data.RoutingQueueId,
//		CaseInboxAssignee:     data.InboxAssignee,
//		CaseOwnerAssignee:     data.OwnerAssignee,
//		CaseEndUserRecipients: caseEndUserRecipients,
//		DetailUrl:             data.DetailUrl,
//		ContactGuid:           data.ContactGuid,
//		CustomerContactID:     data.CustomerContactId,
//		UserInfo:              models.CaseUserInfoView{},
//	}
//
//	createdAt := models.CreatedAtUnix{
//		Seconds: data.CreatedAt, //TODO: convert to Unix() seconds
//		Nanos:   data.CreatedAt, //TODO: convert to UnixNano()
//	}
//
//	caseUpdateEvent := models.CaseUpdateEvent{
//		EventID:   uuid.NewString(),
//		CreatedAt: createdAt,
//		Type:      1,
//		Brand:     brandView,
//		Channel:   channelView,
//		Case:      caseView,
//	}
//}

//var tenantArr [...]string
//tenantArr[0] = "11EB505F-7844-7680-923B-0242AC110003" //15572	perm_DFI_OSH_DO74
//tenantArr[1] = "11EB664D-C2B5-EE70-8733-0242AC110002" //15576	perm_DFI-BillingcycleStart8
//tenantArr[2] = "11EB664E-03B7-9FE0-8733-0242AC110002" //15577	perm_DFI_BillingCycleStart23
