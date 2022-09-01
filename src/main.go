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
	tenantGuidRequest   string = "tenantID (in GUID format)"
	clientIdRequest     string = "clientId"
	clientSecretRequest string = "clientSecret"
)

const CONTACTENDED = "ContactEnded"
const CONTACTSTATEUPDATED = "ContactStateUpdated"

func main() {
	ctx := context.Background()
	// TODO: Call get tenants to get list of tenants that DMW is aware of
	// TODO: Loop through each tenant to get the active contacts, compare, and then update DMW contact states (how can we authenticate if we do this?)
	// TODO: If can't do above todo's, figure out how to input multiple tenants in command line
	//Prompt for tenantId to update
	validTenantId := false
	var tenants [1]string
	tenantID := GetTenantData(tenantGuidRequest)

	//Validate a valid GUID was input
	for !validTenantId {
		validTenantId = Validate(tenantID, true, validTenantId)
		if validTenantId {
			tenants[0] = tenantID
			break
		} else {
			tenantID = GetTenantData(tenantGuidRequest)
		}
	}

	// Get list of Digimiddleware known active contacts
	dmwContactStateApiUrl := "http://digi-shared-eks01-na1.omnichannel.staging.internal:8085/digimiddleware/getstatesbytenants"
	dmwResponse := GetDmwActiveContactStateData(dmwContactStateApiUrl, tenants)

	var dmwKnownContacts DmwKnownContacts
	if dmwResponse != nil {
		err := json.Unmarshal(dmwResponse, &dmwKnownContacts)
		if err != nil {
			fmt.Printf("Cannot unmarshal dmwResponse.  Error: %v", err)
		}
	}

	sort.SliceStable(dmwKnownContacts.Contacts, func(i, j int) bool {
		return dmwKnownContacts.Contacts[i].ContactID < dmwKnownContacts.Contacts[j].ContactID
	})

	// Get DFO auth token
	dfoAuthTokenApiUrl := "https://api-de-na1.staging.niceincontact.com/engager/2.0/token"
	dfoAuthTokenObj := GetDfoAuthToken(dfoAuthTokenApiUrl)

	// Get list of DFO active contacts
	dfoContactSearchApiUrl := "https://api-de-na1.staging.niceincontact.com/dfo/3.0/contacts?status[]=new&status[]=resolved&status[]=escalated&status[]=pending&status[]=open"
	var err error
	var hits int32 = 25
	var dfoActiveContactList models.DfoContactSearchResponse
	var dfoData []models.DataView

	// Call DFO 3.0 GET Contact Search which returns 1st 25 records
	dfoResponse := GetDfoActiveContactList(dfoContactSearchApiUrl, dfoAuthTokenObj)
	if dfoResponse != nil || len(dfoResponse) > 0 {
		err = json.Unmarshal(dfoResponse, &dfoActiveContactList)
		if err != nil {
			fmt.Printf("Cannot unmarshal dfoResponse to full object.  Error: %v", err)
			return
		}
	} else {
		fmt.Println("GetDfoActiveContactList returned 0 records")
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
					return
				}
			}

			// Append next 25 Data records to Data list
			dfoData = append(dfoData, dfoActiveContactList.Data...)
		}
	}

	sort.SliceStable(dfoData, func(i, j int) bool {
		return dfoData[i].Id < dfoData[j].Id
	})

	// Compare lists to get the contacts that exist in DMW but are closed in DFO.
	deltaList := GetDeltaList(dmwKnownContacts, dfoData)
	var deltaContacts DmwKnownContacts
	if deltaList != nil {
		err := json.Unmarshal(deltaList, &deltaContacts)
		if err != nil {
			fmt.Println("Cannot unmarshal deltaList")
		}
	}
	fmt.Println(deltaContacts.Contacts)

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

// GetTenantCredentials requests user inputs the tenant's clientId into the console for authentication
func GetTenantData(inputType string) string {
	fmt.Printf("Input %v': ", inputType)
	reader := bufio.NewReader(os.Stdin)

	response, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}

	return strings.TrimSuffix(response, "\n")
}

// Validate verifies that the data input by the user was in a proper type or format
func Validate(inputValue string, isGuid bool, inputValueValid bool) bool {
	if isGuid {
		if _, err := uuid.Parse(inputValue); err != nil {
			fmt.Printf("Input value is not a valid GUID: [%v]\n", inputValue)
			return inputValueValid
		}
	} else if len(inputValue) != 45 {
		fmt.Printf("Input value is not the proper string length: [%v]\n", inputValue)
		return inputValueValid
	}

	inputValueValid = true
	return inputValueValid
}

// GetDmwActiveContactStateData calls the Digimiddleware api POST digimiddleware/getstatesbytenants to get the list of contacts stored in DynamoDB
func GetDmwActiveContactStateData(apiUrl string, tenants [1]string) []byte {
	contentType := "application/json"
	var tenantIdsObj TenantIdsObj
	tenantIdsObj.TenantIDs = tenants
	bodyJson, _ := json.Marshal(tenantIdsObj)
	reader := bytes.NewReader(bodyJson)
	var responseData []byte

	response, err := http.Post(apiUrl, contentType, reader)

	if err != nil {
		fmt.Print(err.Error())
		os.Exit(1)
	}

	if response.StatusCode == 200 {
		responseData, err = ioutil.ReadAll(response.Body)
		if err != nil {
			log.Fatal(err)
		}
	}
	return responseData
}

// GetDfoActiveContactList calls DFO 3.0 api GET Contact Search which returns a list of active contacts for tenant auth token provided
func GetDfoActiveContactList(apiUrl string, dfoAuthTokenObj DfoAuthTokenObj) []byte {
	var responseData []byte

	// Create a Bearer string by appending string access token
	var bearer = dfoAuthTokenObj.TokenType + " " + dfoAuthTokenObj.AccessToken

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

// GetDfoAuthToken calls the DFO api POST engager/2.0/token to get a bearer token for subsequent DFO api calls
func GetDfoAuthToken(apiUrl string) DfoAuthTokenObj {
	contentType := "application/json"
	var dfoAuthTokenBody DfoAuthTokenBody
	dfoAuthTokenBody.GrantType = "client_credentials"

	clientId := GetTenantData(clientIdRequest)

	//Validate a valid ClientId was input (just checks string length)
	validClientId := false
	for !validClientId {
		validClientId = Validate(clientId, false, validClientId)
		if validClientId {
			dfoAuthTokenBody.ClientId = clientId
			break
		} else {
			clientId = GetTenantData(clientIdRequest)
		}
	}

	clientSecret := GetTenantData(clientSecretRequest)

	//Validate a valid ClientSecret was input (just checks string length)
	validClientSecret := false
	for !validClientSecret {
		validClientSecret = Validate(clientSecret, false, validClientSecret)
		if validClientSecret {
			dfoAuthTokenBody.ClientSecret = clientSecret
			break
		} else {
			clientSecret = GetTenantData(clientSecretRequest)
		}
	}

	bodyJson, _ := json.Marshal(dfoAuthTokenBody)
	reader := bytes.NewReader(bodyJson)
	var responseData []byte

	response, err := http.Post(apiUrl, contentType, reader)

	if err != nil {
		fmt.Print(err.Error())
		os.Exit(1)
	}

	if response.StatusCode == 200 {
		responseData, err = ioutil.ReadAll(response.Body)
		if err != nil {
			log.Fatal(err)
		}
	}

	var dfoAuthTokenObj DfoAuthTokenObj
	if responseData != nil {
		err := json.Unmarshal(responseData, &dfoAuthTokenObj)
		if err != nil {
			fmt.Printf("Cannot unmarshal dfoAuthToken. Error: %v", err)
		}
	} else {
		fmt.Println("DfoAuthToken was null or empty.")
	}
	return dfoAuthTokenObj
}

// GetDeltaList loops through the known contacts from DMW and compares them to the active contacts in DFO and creates a list of contacts that need to be updated
func GetDeltaList(dmwKnownContacts DmwKnownContacts, dfoData []models.DataView) []byte {
	type DmwContacts []DmwKnownContact
	deltaArray := DmwContacts{}
	var deltaList DmwKnownContacts

	// Loop through DmwKnownContacts and check each contact data in DfoActiveContacts to see if we find a match
	for _, contact := range dmwKnownContacts.Contacts {
		found := false
		requiresUpdate := false
		var realContactState db.InDataContactState
		var data models.DataView
		// Only compare contact with DFO data if contact is not closed (18)
		if contact.CurrentContactState != 18 {
			// Compare the data from DFO with the DMW data
			for _, data = range dfoData {
				dataId, _ := strconv.ParseInt(data.Id, 10, 64)
				// Check if there is a match for an active contact in DfoData
				if contact.ContactID == dataId {
					found = true
					// Determine what the correct current contact state should be based on the dfoData.
					realContactState = determineContactStateFromData(data.InboxAssigneeUser.IncontactId, data.RoutingQueueId, data.Status)
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

			contact.CurrentContactDate = time.Now().String() //TODO: do we have to find the real time it was closed?
			contact.EventID = uuid.NewString()

			if found {
				contact.CurrentContactState = int32(realContactState)
			} else {
				contact.CurrentContactState = int32(db.InDataContactState_EndContact)
			}

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

	reqBodyBytes := new(bytes.Buffer)
	json.NewEncoder(reqBodyBytes).Encode(deltaList)
	return reqBodyBytes.Bytes()
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

//var tenantArr [...]string
//tenantArr[0] = "11EB505F-7844-7680-923B-0242AC110003" //15572	perm_DFI_OSH_DO74
//tenantArr[1] = "11EB664D-C2B5-EE70-8733-0242AC110002" //15576	perm_DFI-BillingcycleStart8
//tenantArr[2] = "11EB664E-03B7-9FE0-8733-0242AC110002" //15577	perm_DFI_BillingCycleStart23
