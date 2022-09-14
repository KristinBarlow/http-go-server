package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/MyGoProjects/http-go-server/src/models"
	"github.com/google/uuid"
	pbm "github.com/inContact/orch-common/proto/digi/digimiddleware"
	"github.com/inContact/orch-digital-middleware/pkg/digierrors"
	"github.com/inContact/orch-digital-middleware/pkg/digiservice"
	"github.com/inContact/orch-digital-middleware/pkg/digitransport"
	"github.com/pkg/errors"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	dmwGetStatesEndpoint            = "/getstatesbytenants"
	dfoAuthTokenEndpoint            = "/token"
	dfoContactSearchEndpoint        = "/contacts?status[]=new&status[]=resolved&status[]=escalated&status[]=pending&status[]=open"
	dfoContactByIdEndpoint          = "/contacts/"
	DMWAPIURLPREFIX          string = "http://digi-shared-eks01-"
	DFOAPIURLPREFIX          string = "https://api-de-"
	DMWGRPCURIPREFIX         string = "digi-shared-eks01-"
	regionRequest            string = "region - \"na1\" (Oregon), \"au1\" (Australia), \"eu1\" (Frankfurt), \"jp1\" (Japan), \"uk1\" (London), \"ca1\" (Montreal)"
	envRequest               string = "environment - \"dev\", \"test\", \"staging\", \"prod\""
	tenantGuidRequest        string = "tenantID (in GUID format)"
	busNoRequest             string = "business unit number"
	clientIdRequest          string = "clientId"
	clientSecretRequest      string = "clientSecret"
	batchSize                int    = 100
)

func main() {
	st := time.Now()
	region, env, tenantId, busNo, clientId, clientSecret := "", "", "", "", "", ""
	var tenants [1]string
	var wg sync.WaitGroup

	//Prompt for needed input data
	region = PromptForInputData("region", regionRequest)
	env = PromptForInputData("env", envRequest)
	tenantId = PromptForInputData("tenantId", tenantGuidRequest)
	busNo = PromptForInputData("busNo", busNoRequest)
	clientId = PromptForInputData("clientCreds", clientIdRequest)
	clientSecret = PromptForInputData("clientCreds", clientSecretRequest)

	// This section used for debugging.  Comment out prompts above and uncomment below to fill in data.
	//region = "na1"
	//env = "dev"
	//tenantId = "11EB505F-7844-7680-923B-0242AC110003"
	//busNo = "15572"
	//clientId = "cNqAL5N8CyGT5oMTTczpdSeDQnxTN2hyeg4m4QAqjY2s5"
	//clientSecret = "2ZLdiztGgAcPlHunzTX355M8Hx9fqHunNvXctE0iWYBru"

	// Build api call URIs
	dmwContactStateApiUrl := BuildUri("dmwGetStates", DMWAPIURLPREFIX, region, env)
	dfoAuthTokenApiUrl := BuildUri("dfoAuthToken", DFOAPIURLPREFIX, region, env)
	dfoContactSearchApiUrl := BuildUri("dfoContactSearch", DFOAPIURLPREFIX, region, env)
	dfoContactByIdApiUrl := BuildUri("dfoContactById", DFOAPIURLPREFIX, region, env)
	dmwGrpcApiUrl := BuildUri("dmwGrpc", DMWGRPCURIPREFIX, region, env)

	// Get DFO auth token
	var dfoAuthTokenObj DfoAuthTokenObj
	wg.Add(1)
	go func() {
		defer wg.Done()
		var cancel context.CancelFunc
		ctx := context.Background()
		var err error
		t := time.Now()
		ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
		fmt.Println("begin api call: GetDfoAuthToken")
		dfoAuthTokenObj, err = GetDfoAuthToken(ctx, dfoAuthTokenApiUrl, clientId, clientSecret)
		if err != nil {
			fmt.Printf("error calling GetDfoAuthToken: [%v]\n", err)
			return
		}
		cancel()
		fmt.Printf("%s - done, duration=%s\n", "GetDfoAuthToken", time.Since(t))
	}()
	wg.Wait()

	// Get list of Digimiddleware known active contacts
	var dmwKnownContacts DmwKnownContacts

	wg.Add(1)
	go func() {
		defer wg.Done()
		var cancel context.CancelFunc
		ctx := context.Background()
		var err error
		t := time.Now()
		tenants[0] = tenantId
		ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
		fmt.Println("begin api call: GetDmwActiveContactStateData")
		dmwKnownContacts, err = GetDmwActiveContactStateData(ctx, dmwContactStateApiUrl, tenants)
		if err != nil {
			fmt.Printf("error calling GetDmwActiveContactStateData: [%v]\n", err)
			return
		}
		cancel()
		fmt.Printf("%s - done, duration=%s, contacts=%d\n", "GetDmwActiveContactStateData", time.Since(t), len(dmwKnownContacts.Contacts))
	}()

	// Call DFO 3.0 GET Contact Search API to get list of DFO active contacts
	var dfoData []models.DataView
	wg.Add(1)
	go func() {
		defer wg.Done()
		var cancel context.CancelFunc
		ctx := context.Background()
		var err error
		t := time.Now()
		ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
		fmt.Println("begin api call: MakeDfoContactSearchApiCall")
		dfoData, err = MakeDfoContactSearchApiCall(ctx, dfoContactSearchApiUrl, dfoAuthTokenObj)
		if err != nil {
			fmt.Printf("error calling MakeDfoContactSearchApiCall: [%v]\n", err)
			return
		}
		cancel()
		fmt.Printf("%s - done, duration=%s, dfoActiveContacts=%d\n", "MakeDfoContactSearchApiCall", time.Since(t), len(dfoData))
	}()
	wg.Wait()

	// Compare lists to get the contacts that exist in DMW but are closed in DFO.
	var deltaContacts DmwKnownContacts

	if dmwKnownContacts.Contacts != nil {
		fmt.Println("begin building list: buildDeltaList")
		t := time.Now()
		deltaContacts = buildDeltaList(dmwKnownContacts, dfoData)
		fmt.Printf("%s - done, duration=%s, deltaContacts=%d\n", "buildDeltaList", time.Since(t), len(deltaContacts.Contacts))
	} else {
		fmt.Println("there were no contacts in dmw list - no need to process updates")
	}

	// Batch and Process records to digimiddleware using gRPC
	if len(deltaContacts.Contacts) > 0 {
		process(deltaContacts.Contacts, dfoAuthTokenObj, dfoContactByIdApiUrl, dmwGrpcApiUrl, tenantId, busNo)
	} else {
		fmt.Println("comparison of lists returned 0 contacts to update - no need to process updates")
	}

	fmt.Printf("update case state service completed - totalDuration = %s\n", time.Since(st))
}

// process batches the data into specified batchSize to process
func process(data []DmwKnownContact, dfoAuthTokenObj DfoAuthTokenObj, dfoContactByIdApiUrl, dmwGrpcApiUrl, tenantId, busNo string) {
	var batchCount int
	for start, end := 0, 0; start <= len(data)-1; start = end {
		var err error
		var sanitizedUpdateRecords []*pbm.CaseUpdateEvent

		end = start + batchSize
		if end > len(data) {
			end = len(data)
		}
		batch := data[start:end]

		t := time.Now()
		sanitizedUpdateRecords = processBatch(batch, dfoContactByIdApiUrl, dfoAuthTokenObj, tenantId, busNo)
		fmt.Printf("%s [%d] - done, duration=%s, total records to update=%d\n", "processBatch", batchCount, time.Since(t), len(sanitizedUpdateRecords))
		batchCount++

		// Push sanitizedUpdateRecords to digimiddleware via gRPC
		if sanitizedUpdateRecords != nil {
			fmt.Println("grpc call start: sendUpdateRecordsToMiddleware")
			t := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			err = sendUpdateRecordsToMiddleware(ctx, &pbm.CaseUpdateEvents{
				Updates:    sanitizedUpdateRecords,
				ReceivedAt: timestamppb.Now(),
			}, dmwGrpcApiUrl)

			if err != nil {
				fmt.Printf("error making grpc call to send update records to middleware: [%v]\n", err)
				return
			}
			cancel()
			fmt.Printf("%s - done, duration=%s, count=%d\n", "sendUpdateRecordsToMiddleware", time.Since(t), len(sanitizedUpdateRecords))
		} else {
			fmt.Println("there were no contacts added to sanitizedUpdateRecords list - no need to process updates")
		}
	}
}

// processBatch calls DFO 3.0 GET Contact to obtain contact data to build the case update event
func processBatch(list []DmwKnownContact, dfoContactByIdApiUrl string, dfoAuthTokenObj DfoAuthTokenObj, tenantId, busNo string) []*pbm.CaseUpdateEvent {
	var count int32
	var mtx sync.Mutex
	var sanitizedUpdateRecords []*pbm.CaseUpdateEvent
	var wg sync.WaitGroup

	fmt.Printf("begin processing batch %d\n", count)
	for _, contact := range list {
		count++
		wg.Add(1)
		go func(contact DmwKnownContact, count int32) {
			var contactData models.DataView
			var updateMessage *pbm.CaseUpdateEvent
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)

			// Call DFO 3.0 GET Contacts by contactId for each record in Delta list to obtain actual metadata for contact
			contactData = MakeDfoContactByIdApiCall(ctx, dfoContactByIdApiUrl, dfoAuthTokenObj, contact)

			if contactData.Id != "" {
				// Create the Update Event object from the contactData received from DFO
				event, err := createEvent(contactData, tenantId, busNo)
				if err != nil {
					fmt.Println(err)
					return
				} else {
					fmt.Printf("event object created for contactId: [%v]\n", event.Data.Case.ID)
				}

				// Using the Event Object, create the CaseUpdateEvent object
				if event.Data.Case.ID != "" {
					updateMessage, err = makeCaseUpdate(event)
					if err != nil {
						fmt.Println(err)
						return
					} else {
						fmt.Printf("update event object created: brand: %+v, case: %+v, channel: %+v, createdAt: %+v, eventId: %v\n", updateMessage.Brand, updateMessage.Case, updateMessage.Channel, updateMessage.CreatedAt, updateMessage.EventID)
					}

					if updateMessage != nil {
						mtx.Lock()
						sanitizedUpdateRecords = append(sanitizedUpdateRecords, updateMessage)
						mtx.Unlock()
					}
				} else {
					fmt.Printf("unable to create event object: contact was empty - contactId: %v\n", contact.ContactID)
				}
			}
			cancel()
		}(contact, count)
	}
	wg.Wait()

	return sanitizedUpdateRecords
}

func BuildUri(apiType string, apiPrefix string, region string, env string) string {
	fmt.Printf("%s uri for requested region [%s] and env [%s] -- ", apiType, region, env)
	uri := ""
	switch apiType {
	case "dmwGetStates":
		uri = apiPrefix + region + ".omnichannel." + env + ".internal:8085/digimiddleware/getstatesbytenants"
		fmt.Println(uri)
	case "dfoAuthToken":
		switch env {
		case "prod":
			uri = apiPrefix + region + ".niceincontact.com/engager/2.0/token"
			fmt.Println(uri)
		case "dev", "test", "staging":
			uri = apiPrefix + region + "." + env + ".niceincontact.com/engager/2.0/token"
			fmt.Println(uri)
		default:
			break
		}
	case "dfoContactSearch":
		switch env {
		case "prod":
			uri = apiPrefix + region + ".niceincontact.com/dfo/3.0/contacts?status[]=new&status[]=resolved&status[]=escalated&status[]=pending&status[]=open"
			fmt.Println(uri)
		case "dev", "test", "staging":
			uri = apiPrefix + region + "." + env + ".niceincontact.com/dfo/3.0/contacts?status[]=new&status[]=resolved&status[]=escalated&status[]=pending&status[]=open"
			fmt.Println(uri)
		default:
			break
		}
	case "dfoContactById":
		switch env {
		case "prod":
			uri = apiPrefix + region + ".niceincontact.com/dfo/3.0/contacts/"
			fmt.Println(uri)
		case "dev", "test", "staging":
			uri = apiPrefix + region + "." + env + ".niceincontact.com/dfo/3.0/contacts/"
			fmt.Println(uri)
		default:
			break
		}
	case "dmwGrpc":
		uri = apiPrefix + region + ".omnichannel." + env + ".internal:9884"
		fmt.Println(uri)
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
	fmt.Printf("input %v: ", inputType)
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
			fmt.Println("Input value is not a valid region name: (\"na1\", \"au1\", \"eu1\", \"jp1\", \"uk1\", \"ca1\")")
			inputValueValid = false
		}
	case "env":
		switch strings.ToLower(inputValue) {
		case "dev", "test", "staging", "prod":
			inputValueValid = true
		default:
			fmt.Println("Input value is not a valid environment name: (\"dev\", \"test\", \"staging\", \"prod\")")
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
	case "busNo":
		if _, err := strconv.ParseInt(inputValue, 10, 32); err != nil {
			fmt.Printf("Input value is not a valid integer: [%v]\n", inputValue)
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
func GetDmwActiveContactStateData(ctx context.Context, apiUrl string, tenants [1]string) (DmwKnownContacts, error) {
	var dmwKnownContacts DmwKnownContacts
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
		return dmwKnownContacts, err
	}

	if response.StatusCode == 200 {
		responseData, err = ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Print(err.Error())
			return dmwKnownContacts, err
		}
	}

	if responseData != nil {
		err := json.Unmarshal(responseData, &dmwKnownContacts)
		if err != nil {
			fmt.Printf("cannot unmarshal dmw response: [%v]\n", err)
			return dmwKnownContacts, err
		}
	}

	sort.SliceStable(dmwKnownContacts.Contacts, func(i, j int) bool {
		return dmwKnownContacts.Contacts[i].ContactID < dmwKnownContacts.Contacts[j].ContactID
	})

	return dmwKnownContacts, err
}

// GetDfoAuthToken calls the DFO api POST engager/2.0/token to get a bearer token for subsequent DFO api calls
func GetDfoAuthToken(ctx context.Context, apiUrl string, clientId string, clientSecret string) (DfoAuthTokenObj, error) {
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
			fmt.Print(err.Error())
			return dfoAuthTokenObj, err
		}
	}

	if responseData != nil {
		err := json.Unmarshal(responseData, &dfoAuthTokenObj)
		if err != nil {
			fmt.Printf("cannot unmarshal dfo auth token: [%v]\n", err)
			return dfoAuthTokenObj, err
		}
	} else {
		fmt.Println("dfo auth token was null or empty")
		return dfoAuthTokenObj, err
	}

	fmt.Println("dfo auth token successfully retrieved")

	return dfoAuthTokenObj, err
}

// MakeDfoContactSearchApiCall calls the DFO 3.0 GET Contact Search Api then loops until all data is retrieved (api only returns 25 records at a time)
func MakeDfoContactSearchApiCall(ctx context.Context, dfoContactSearchApiUrl string, dfoAuthTokenObj DfoAuthTokenObj) ([]models.DataView, error) {
	var dfoActiveContactList models.DfoContactSearchResponse
	var dfoData []models.DataView
	var err error
	var hits int32 = 25
	var mtx sync.Mutex

	// Call DFO 3.0 GET Contact Search which returns 1st 25 records
	dfoResponse, err := MakeDfoApiCall(ctx, dfoContactSearchApiUrl, dfoAuthTokenObj, "")
	if err != nil {
		return dfoData, err
	}
	if dfoResponse != nil || len(dfoResponse) > 0 {
		err := json.Unmarshal(dfoResponse, &dfoActiveContactList)
		if err != nil {
			fmt.Printf("cannot unmarshal dfo response to full object: [%v]\n", err)
			return dfoData, err
		}
	} else {
		fmt.Println("GetDfoActiveContactList returned 0 records")
		return dfoData, err
	}

	fmt.Printf("GetDfoActiveContactList returned [%v] total hits\n", dfoActiveContactList.Hits)

	// Append first 25 Data records to Data list
	mtx.Lock()
	dfoData = append(dfoData, dfoActiveContactList.Data...)
	mtx.Unlock()

	if dfoActiveContactList.Hits > 25 {
		// Sleep for 1 sec between calls to not overload the GET Contact Search api
		time.Sleep(100 * time.Millisecond)
		for hits <= dfoActiveContactList.Hits {
			dfoContactSearchApiUrlSt := dfoContactSearchApiUrl + "&scrollToken=" + dfoActiveContactList.ScrollToken
			hits += 25

			// Call DFO 3.0 GET Contact Search to get next 25 records
			fmt.Printf("calling GetDmwActiveContactStateData to get next set of records up to [%v]\n", hits)
			dfoResponse, err = MakeDfoApiCall(ctx, dfoContactSearchApiUrlSt, dfoAuthTokenObj, "")
			if err != nil {
				return dfoData, err
			}

			if dfoResponse != nil {
				err := json.Unmarshal(dfoResponse, &dfoActiveContactList)
				if err != nil {
					fmt.Printf("Cannot unmarshal dfo response to full object.  Error: [%v]\n", err)
					return dfoData, err
				}
			}

			// Append next set of Data records to Data list
			mtx.Lock()
			dfoData = append(dfoData, dfoActiveContactList.Data...)
			mtx.Unlock()
		}
	}
	sort.SliceStable(dfoData, func(i, j int) bool {
		return dfoData[i].Id < dfoData[j].Id
	})

	return dfoData, err
}

// buildDeltaList loops through the known contacts from DMW and compares them to the active contacts in DFO and creates a list of contacts that need to be updated
func buildDeltaList(dmwKnownContacts DmwKnownContacts, dfoData []models.DataView) DmwKnownContacts {
	type DmwContacts []DmwKnownContact
	deltaArray := DmwContacts{}
	var deltaList DmwKnownContacts
	foundCount := 0
	notFoundCount := 0
	alreadyClosedCount := 0

	// Loop through DmwKnownContacts and check each contact data in DfoActiveContacts to see if we find a match
	for _, contact := range dmwKnownContacts.Contacts {
		found := false
		shouldClose := false
		// Only compare contact with DFO data if contact is not closed (18)
		if contact.CurrentContactState != 18 {
			// Compare the data from DFO with the DMW data
			for _, d := range dfoData {
				shouldClose = false
				dataId, _ := strconv.ParseInt(d.Id, 10, 64)
				if contact.ContactID == dataId {
					found = true
					foundCount++
					fmt.Printf("ContactID*[%d]*Found*[%v]*ShouldClose*[%v]*DmwContactState*[%d]*DfoContactState*[%s]*CurrentContactDate*[%v]\n", contact.ContactID, found, shouldClose, contact.CurrentContactState, d.Status, contact.CurrentContactDate)
					break
				} else {
					shouldClose = true
				}
			}

			if !found {
				notFoundCount++
				fmt.Printf("ContactID*[%d]*Found*[%v]*ShouldClose*[%v]*DmwContactState*[%d]*DfoContactState*[%d]*CurrentContactDate*[%v]\n", contact.ContactID, found, shouldClose, contact.CurrentContactState, 18, contact.CurrentContactDate)
			}
		} else {
			alreadyClosedCount++
		}

		// If no match is found and not already closed, add contact data to deltaList
		if shouldClose {
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
				PreviousQueueID:             contact.PreviousQueueID,
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
		}
	}

	fmt.Printf("total dmwKnownContacts: %d\n", len(dmwKnownContacts.Contacts))
	fmt.Printf("total contacts that were already closed (will not be updated): %d\n", alreadyClosedCount)
	fmt.Printf("total contacts that were not found (will be updated): %d\n", notFoundCount)
	fmt.Printf("total contacts that were found (will not be updated): %d\n", foundCount)
	return deltaList
}

// MakeDfoContactByIdApiCall
func MakeDfoContactByIdApiCall(ctx context.Context, dfoContactByIdApiUrl string, dfoAuthTokenObj DfoAuthTokenObj, contact DmwKnownContact) models.DataView {
	var dfoClosedContactData models.DataView

	dfoResponse, respErr := MakeDfoApiCall(ctx, dfoContactByIdApiUrl, dfoAuthTokenObj, strconv.Itoa(int(contact.ContactID)))
	if respErr != nil {
		fmt.Println(respErr)
	} else if dfoResponse != nil || len(dfoResponse) > 0 {
		err := json.Unmarshal(dfoResponse, &dfoClosedContactData)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("success: received dfo data for contactId: [%d]\n", contact.ContactID)
	}

	return dfoClosedContactData
}

// MakeDfoApiCall calls DFO 3.0 APIs and returns the response object
func MakeDfoApiCall(ctx context.Context, apiUrl string, dfoAuthTokenObj DfoAuthTokenObj, contactId string) ([]byte, error) {
	var bearer = dfoAuthTokenObj.TokenType + " " + dfoAuthTokenObj.AccessToken
	var responseData []byte

	if contactId != "" {
		apiUrl = apiUrl + contactId
	}

	// Create a new request using http
	req, err := http.NewRequest("GET", apiUrl, nil)

	if err != nil {
		fmt.Printf("new http request returned an error: [%v]\n", err)
		return responseData, err
	}
	if req != nil {
		// add authorization header and content-type to the req
		req.Header.Add("Authorization", bearer)
		req.Header.Add("Content-Type", "application/json")
	}

	// Send req using http Client
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("error connecting to host\n[ERROR] - %v\n", err)
		return responseData, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		responseData, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("error reading response bytes: %v\n", err)
			return responseData, err
		}
	} else {
		err = fmt.Errorf("error calling dfo api for contact %s: %v", contactId, resp.Status)
		return responseData, err
	}

	return responseData, err
}

//func removeFromDeltaList(i int32, contact DmwKnownContact, deltaListContacts []DmwKnownContact) []DmwKnownContact {
//	fmt.Printf("removing contact from deltaList: [%d] - ", contact.ContactID)
//	a := deltaListContacts
//	var x DmwKnownContact
//
//	copy(a[i:], a[i+1:]) // Shift a[i+1:] left one index.
//	a[len(a)-1] = x      // Erase last element (write zero value - x is empty dataType).
//	//a = a[:len(a)-1]     // Truncate slice.
//
//	fmt.Printf("new length after removing contact: [%d]\n", len(a))
//	return a
//}

func createEvent(contactData models.DataView, tenantID, busNo string) (models.StreamEventRequest, error) {
	b, _ := strconv.ParseInt(busNo, 10, 32)
	businessUnitNo := int32(b)
	var err error
	var event models.StreamEventRequest

	if contactData.Error == nil {
		channelParts := strings.Split(contactData.ChannelId, "_")

		event.CreatedAt = contactData.CreatedAt
		event.EventID = uuid.New().String()
		event.EventObject = models.EventObject_Case
		event.EventType = 1
		event.Data.Brand.ID = 9999 //we don't use this so hardCode anything
		event.Data.Brand.TenantID = tenantID
		event.Data.Brand.BusinessUnitID = businessUnitNo
		event.Data.Case.ID = contactData.Id
		event.Data.Case.ContactId = contactData.ContactGuid
		event.Data.Case.CustomerContactId = contactData.CustomerContactId
		event.Data.Case.DetailUrl = contactData.DetailUrl
		event.Data.Case.EndUserRecipients = contactData.EndUserRecipients
		event.Data.Case.InboxAssignee = contactData.InboxAssignee
		event.Data.Case.InteractionId = contactData.InteractionId
		event.Data.Case.Direction = contactData.Direction
		event.Data.Case.ThreadId = contactData.ThreadId
		event.Data.Case.RoutingQueueId = contactData.RoutingQueueId
		event.Data.Case.RoutingQueuePriority = contactData.RoutingQueuePriority
		event.Data.Case.Status = contactData.Status
		event.Data.Case.OwnerAssignee = contactData.OwnerAssignee
		event.Data.Channel.ID = contactData.ChannelId
		event.Data.Channel.Name = "channelName"
		event.Data.Channel.IntegrationBoxIdentifier = channelParts[0]
		event.Data.Channel.IDOnExternalPlatform = channelParts[1]
		event.Data.Channel.IsPrivate = true
		event.Data.Channel.RealExternalPlatformID = channelParts[0]

		fmt.Printf("update event list object:\n %+v\n", event.Data.Case)
	} else {
		err = fmt.Errorf("unable to create update event object for contactId: %s, tenant: %s. MakeDfoContactByIdApiCall returned error: %v", contactData.Id, contactData.TenantID, contactData.Error)
	}

	return event, err
}

// Turn the StreamEventRequest into a CaseUpdateEvent protobuf object to be sent via GRPC
func makeCaseUpdate(event models.StreamEventRequest) (*pbm.CaseUpdateEvent, error) {
	var err error
	message := pbm.CaseUpdateEvent{
		EventID:   event.EventID,
		CreatedAt: getCreatedAt(event),
		Brand: &pbm.Brand{
			ID:             event.Data.Brand.ID,
			TenantID:       event.Data.Brand.TenantID,
			BusinessUnitID: event.Data.Brand.BusinessUnitID,
		},
		Channel: &pbm.Channel{
			ID:                       event.Data.Channel.ID,
			Name:                     event.Data.Channel.Name,
			IntegrationBoxIdentifier: event.Data.Channel.IntegrationBoxIdentifier,
			IdOnExternalPlatform:     event.Data.Channel.IDOnExternalPlatform,
			IsPrivate:                event.Data.Channel.IsPrivate,
			RealExternalPlatformID:   event.Data.Channel.RealExternalPlatformID,
		},
		Case: makeCase(event),
		Type: pbm.CaseUpdateType_TYPE_STATUS_CHANGED,
	}

	if message.Case == nil || message.Brand.TenantID == "" {
		err = fmt.Errorf("non-shippable case update event - case or tenant id empty - case: %+v, tenantId: %s\n", message.Case, message.Brand.TenantID)
		return nil, err
	}

	fmt.Printf("case update message:\n %+v\n", message.Case)

	return &message, nil
}

func getCreatedAt(event models.StreamEventRequest) (ts *timestamppb.Timestamp) {
	if event.CreatedAtWithMilliseconds != nil {
		return event.CreatedAtWithMilliseconds.Timestamp()
	}
	return event.CreatedAt.Timestamp()
}

func makeCase(event models.StreamEventRequest) (caseEvent *pbm.Case) {
	caseEvent = &pbm.Case{
		ID:                   event.Data.Case.ID,
		ContactGuid:          event.Data.Case.ContactId,
		CustomerContactID:    event.Data.Case.CustomerContactId,
		DetailUrl:            event.Data.Case.DetailUrl,
		EndUserRecipients:    recipientMap(event.Data.Case.EndUserRecipients),
		InboxAssignee:        event.Data.Case.InboxAssignee,
		InteractionID:        event.Data.Case.InteractionId,
		IsOutbound:           strings.ToLower(event.Data.Case.Direction) == "outbound",
		PostID:               event.Data.Case.ThreadId,
		RoutingQueueID:       event.Data.Case.RoutingQueueId,
		RoutingQueuePriority: event.Data.Case.RoutingQueuePriority,
		Status:               event.Data.Case.Status,
		OwnerAssignee:        event.Data.Case.OwnerAssignee,
	}
	if caseEvent.IsOutbound {
		caseEvent.UserInfo = &pbm.Case_AuthorUser{
			AuthorUser: &pbm.User{
				ID:            event.Data.Case.AuthorUser.ID,
				IncontactID:   event.Data.Case.AuthorUser.InContactID,
				EmailAddress:  event.Data.Case.AuthorUser.EmailAddress,
				LoginUsername: event.Data.Case.AuthorUser.LoginUsername,
				FirstName:     event.Data.Case.AuthorUser.FirstName,
				Surname:       event.Data.Case.AuthorUser.SurName,
			},
		}
	} else {
		caseEvent.UserInfo = &pbm.Case_AuthorEndUserIdentity{
			AuthorEndUserIdentity: &pbm.EndUserIdentity{
				ID:                   event.Data.Case.AuthorEndUserIdentity.ID,
				IdOnExternalPlatform: event.Data.Case.AuthorEndUserIdentity.IdOnExternalPlatform,
				FullName:             event.Data.Case.AuthorEndUserIdentity.FullName,
			},
		}
	}
	if caseEvent.ID == "" || caseEvent.Status == "" {
		return nil
	}
	return caseEvent
}

// Turn the StreamEventRequest recipients list into the corresponding Recipients protobuf list.
func recipientMap(recipients []models.Recipient) []*pbm.Recipient {
	results := make([]*pbm.Recipient, len(recipients))
	for i, r := range recipients {
		results[i] = &pbm.Recipient{
			IdOnExternalPlatform: r.IdOnExternalPlatform,
			Name:                 r.Name,
			IsPrimary:            r.IsPrimary,
			IsPrivate:            r.IsPrivate,
		}
	}
	return results
}

func sendUpdateRecordsToMiddleware(ctx context.Context, events *pbm.CaseUpdateEvents, dmwGrpcApiUrl string) error {
	if len(events.Updates) == 0 {
		fmt.Println("no case update events were created")
		return nil
	} else {
		fmt.Printf("total count of case event updates, [%v]\n", len(events.Updates))
	}

	op := digierrors.Op("sendUpdateRecordsToMiddleware")

	// Create gRPC client to pass CaseEventUpdate to digimiddlware
	fmt.Println("begin grpc call: createGrpcClient")
	t := time.Now()
	middlewareEventService := createGrpcClient(ctx, dmwGrpcApiUrl)
	fmt.Printf("%s - done, duration=%s\n", "createGrpcClient", time.Since(t))

	if middlewareEventService == nil {
		return nil
	}

	fmt.Println("begin grpc call: CaseEventUpdate")
	t = time.Now()
	response, err := middlewareEventService.CaseEventUpdate(ctx, events)
	fmt.Printf("%s - done, duration=%s\n", "CaseEventUpdate", time.Since(t))

	if err != nil {
		// Failure to deliver to Middleware (e.g., network errors, etc.)
		return digierrors.E(op, digierrors.IsRetryable, zapcore.ErrorLevel, err)
	}

	// If we got an error response, then the Middleware indicates this is retryable. Return an error here.
	errStr := response.GetErr()
	if errStr != "" {
		fmt.Printf("received error from case update grpc: [%v]\n", errStr)
		return digierrors.E(op, digierrors.IsRetryable, zapcore.ErrorLevel, errors.New(errStr))
	}
	fmt.Printf("wrote case update records to grpc - records count: [%v]\n", len(events.Updates))
	return nil
}

func createGrpcClient(ctx context.Context, dmwGrpcApiUrl string) digiservice.GrpcService {
	digimiddlewareGrpcAddr := dmwGrpcApiUrl
	conn, err := grpc.Dial(digimiddlewareGrpcAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("initialization of digimiddleware grpc client failed with err: [%v]\n", err)
		return nil
	} else {
		fmt.Println("grpc client initialized")
	}

	middlewareEventService := digitransport.NewGRPCClient(conn, nil, nil)
	return middlewareEventService
}
