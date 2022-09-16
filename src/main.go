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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// Batch size for processing contacts to update (arbitrarily chose 100 as it seemed safe and manageable)
	batchSize = 100

	// DFO api url path variables
	dfoApiUrlPrefix          = "https://api-de-"
	dfoApiV2Path             = ".niceincontact.com/engager/2.0"
	dfoApiV3Path             = ".niceincontact.com/dfo/3.0"
	dfoAuthTokenEndpoint     = "/token"
	dfoContactSearchEndpoint = "/contacts?status[]=new&status[]=resolved&status[]=escalated&status[]=pending&status[]=open"
	dfoContactByIdEndpoint   = "/contacts/"

	// DMW api url path variables
	dmwApiUrlPrefix      = "http://digi-shared-eks01-"
	dmwApiPath           = "/digimiddleware"
	dmwApiPort           = "8085"
	dmwGetStatesEndpoint = "/getstatesbytenants"

	// DMW gRPC path variables
	dmwGrpcUriPrefix = "digi-shared-eks01-"
	dmwGrpcPort      = "9884"

	// "Prompt for input" string variables in order they are prompted
	regionRequest       = "region - \"na1\" (Oregon), \"au1\" (Australia), \"eu1\" (Frankfurt), \"jp1\" (Japan), \"uk1\" (London), \"ca1\" (Montreal)"
	envRequest          = "environment - \"dev\", \"test\", \"staging\", \"prod\""
	tenantGuidRequest   = "tenantID (in GUID format)"
	busNoRequest        = "business unit number"
	clientIdRequest     = "dfo clientId"
	clientSecretRequest = "dfo clientSecret"
	dateFromRequest     = "input \"FromDate\" using format \"YYYY-mm-dd\" (OPTIONAL: Return for no date filter)"
	dateToRequest       = "\"ToDate\" in format \"YYYY-mm-dd\""
	sortTypeRequest     = "sort order - \"asc\" for ascending order or \"desc\" for descending order"

	// Log string for http responses other than 200
	httpBadResponse = "returned response other than 200 success - response.StatusCode: [%d], response.Status: [%s]\n"
)

type TenantIdsObj struct {
	TenantIDs [1]string `json:"tenantIds"`
}

type DfoApiUrlObj struct {
	DateFrom    string // 2022-05-01
	DateTo      string // 2022-09-15
	ScrollToken string // this is returned in dfo response if there are more than 25 hits
	Sorting     string // createdAt
	SortingType string // asc, desc
	Status      string // new, open, resolved, escalated, pending, closed, trashed
	Url         string
}

func main() {
	var dfoData []models.DataView
	var dfoDataErr models.DataView
	var dmwKnownContacts models.DmwKnownContacts
	// Input variables in order they are requested
	region, env, tenantId, busNo, clientId, clientSecret, dateFrom, dateTo, sortType := "", "", "", "", "", "", "", "", ""
	st := time.Now()
	var tenants [1]string
	var wg sync.WaitGroup

	//Prompt for needed input data
	region = promptForInputData("region", regionRequest)
	env = promptForInputData("env", envRequest)
	tenantId = promptForInputData("tenantId", tenantGuidRequest)
	busNo = promptForInputData("busNo", busNoRequest)
	clientId = promptForInputData("clientCreds", clientIdRequest)
	clientSecret = promptForInputData("clientCreds", clientSecretRequest)
	dateFrom = promptForInputData("dateFrom", dateFromRequest)
	if dateFrom != "" {
		dateTo = promptForInputData("dateTo", dateToRequest)
		sortType = promptForInputData("sortType", sortTypeRequest)
	}

	// This section used for debugging.  Comment out prompts above and uncomment below to fill in data.
	//region = "na1"
	//env = "dev"
	//tenantId = "11EA8B00-FE26-D4C0-8B66-0242AC110005"
	//busNo = "4534531"
	//clientId = "hZtufP76V4QKcWogRiaFHVQx1XGspDPFamH78P8n1xQqt"
	//clientSecret = "SdrPoz7hj0GoEvxwDZweiK21jRBRUNFEfIhlrEKaSBK2t"
	//dateFrom = "2020-08-21" // This BU has bad data prior to 8/21/2020 - api will return 500 internal server error
	//dateTo = ""
	//sortType = "asc"

	//region = "na1"
	//env = "dev"
	//tenantId = "11eb5204-ec4d-a370-a1ba-0242ac110002"
	//busNo = "15573"
	//clientId = "i7ZnwBxZ5d5iSgb9EumUYx6I07ZsShyt2lQGKyVLPbMAF"
	//clientSecret = "00eUgcwMlv3IXd2MacYXsgtyXg4PXx8VdGo3NeRbMrlm3"

	// Build api and gRPC URIs
	dmwContactStateApiUrl := buildUri("dmwGetStates", region, env)
	dfoAuthTokenApiUrl := buildUri("dfoAuthToken", region, env)
	dfoContactSearchApiUrl := buildUri("dfoContactSearch", region, env)
	dfoContactByIdApiUrl := buildUri("dfoContactById", region, env)
	dmwGrpcApiUrl := buildUri("dmwGrpc", region, env)

	// Get DFO auth token
	var dfoAuthTokenObj models.DfoAuthTokenObj
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		method := "getDfoAuthToken"
		t := time.Now()
		fmt.Println("begin api call: getDfoAuthToken")
		dfoAuthTokenObj, err = getDfoAuthToken(dfoAuthTokenApiUrl, clientId, clientSecret)
		if err != nil {
			dfoAuthTokenObj.Error = err
			fmt.Printf("error calling [%s]: [%v]\n", method, err)
			return
		}
		fmt.Printf("[%s] - done, duration=%s\n", method, time.Since(t))
	}()
	wg.Wait()

	// Get list of Digimiddleware known active contacts
	if dfoAuthTokenObj.Error == nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			method := "getDmwActiveContactStateData"
			t := time.Now()
			tenants[0] = tenantId
			fmt.Printf("begin api call: [%s]\n", method)
			dmwKnownContacts, err = getDmwActiveContactStateData(dmwContactStateApiUrl, tenants)
			if err != nil {
				dmwKnownContacts.Error = err
				fmt.Printf("error calling [%s]: [%v]\n", method, err)
				return
			}
			fmt.Printf("[%s] - done, duration=%s, contacts=%d\n", method, time.Since(t), len(dmwKnownContacts.Contacts))
		}()
		wg.Wait()

		// Call DFO 3.0 GET Contact Search API to get list of DFO active contacts
		var err error
		wg.Add(1)
		go func() {
			defer wg.Done()
			method := "makeDfoContactSearchApiCall"
			t := time.Now()
			fmt.Printf("begin api call: [%s]\n", method)
			dfoData, err = makeDfoContactSearchApiCall(dfoContactSearchApiUrl, dateFrom, dateTo, sortType, dfoAuthTokenObj)
			if err != nil {
				dfoDataErr.Err = err
				fmt.Printf("error calling [%s]]: [%v]\n", method, err)
				return
			}
			fmt.Printf("[%s] - done, duration=%s, dfoActiveContacts=%d\n", method, time.Since(t), len(dfoData))
		}()
		wg.Wait()
	}

	// Compare lists to get the contacts that exist in DMW but are closed in DFO.
	var deltaContacts models.DmwKnownContacts
	method := "buildDeltaList"

	if dmwKnownContacts.Error == nil && dfoDataErr.Err == nil {
		if len(dmwKnownContacts.Contacts) > 0 {
			fmt.Printf("begin building list: [%s]\n", method)
			t := time.Now()
			deltaContacts = buildDeltaList(dmwKnownContacts, dfoData)
			fmt.Printf("[%s] - done, duration=%s, deltaContacts=%d\n", method, time.Since(t), len(deltaContacts.Contacts))
		} else {
			fmt.Println("dmw list was empty due to no contacts or error retrieving data from api - will not attempt to process updates")
			return
		}
	} else {
		fmt.Println("[makeDfoContactSearchApiCall] error retrieving data from api - will not attempt to process updates")
		return
	}

	// Batch and Process records to digimiddleware using gRPC
	if len(deltaContacts.Contacts) > 0 {
		process(deltaContacts.Contacts, dfoAuthTokenObj, dfoContactByIdApiUrl, dmwGrpcApiUrl, tenantId, busNo)
	} else {
		fmt.Println("comparison of lists returned 0 contacts to update - will not attempt to process updates")
		return
	}

	fmt.Printf("update case state service completed - totalDuration = %s\n", time.Since(st))
}

// process batches the data into specified batchSize to process
func process(data []models.DmwKnownContact, dfoAuthTokenObj models.DfoAuthTokenObj, dfoContactByIdApiUrl, dmwGrpcApiUrl, tenantId, busNo string) {
	var batchCount int
	var errList []models.DataView
	for start, end := 0, 0; start <= len(data)-1; start = end {
		var err error
		var sanitizedUpdateRecords []*pbm.CaseUpdateEvent

		end = start + batchSize
		if end > len(data) {
			end = len(data)
		}
		batch := data[start:end]

		method := "processBatch"
		t := time.Now()
		fmt.Printf("begin processing batch [%d]\n", batchCount)
		sanitizedUpdateRecords, errList = processBatch(batch, dfoContactByIdApiUrl, dfoAuthTokenObj, tenantId, busNo)
		fmt.Printf("[%s] [%d] - done, duration=%s, total records to update=%d\n", method, batchCount, time.Since(t), len(sanitizedUpdateRecords))
		if errList != nil {
			fmt.Println("ERROR processing batch - will not attempt to update below contacts")
			// Range over errList to print all errors together for more readable logs
			for _, e := range errList {
				fmt.Println(e.Err)
			}
		}
		batchCount++

		// Push sanitizedUpdateRecords to digimiddleware via gRPC
		if sanitizedUpdateRecords != nil {
			method2 := "sendUpdateRecordsToMiddleware"
			fmt.Printf("grpc call start: [%s]\n]", method2)
			t := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			err = sendUpdateRecordsToMiddleware(ctx, &pbm.CaseUpdateEvents{
				Updates:    sanitizedUpdateRecords,
				ReceivedAt: timestamppb.Now(),
			}, dmwGrpcApiUrl)

			if err != nil {
				fmt.Printf("[%s] error making grpc call to send update records to middleware: [%v]\n", method2, err)
				cancel()
				return
			}
			cancel()
			fmt.Printf("[%s] - done, duration=%s, count=%d\n", method2, time.Since(t), len(sanitizedUpdateRecords))
		} else {
			fmt.Printf("[%s] there were no contacts added to sanitizedUpdateRecords list - will not attempt to process updates\n", method)
		}
	}
}

// processBatch calls DFO 3.0 GET Contact to obtain contact data to build the case update event
func processBatch(list []models.DmwKnownContact, dfoContactByIdApiUrl string, dfoAuthTokenObj models.DfoAuthTokenObj, tenantId, busNo string) ([]*pbm.CaseUpdateEvent, []models.DataView) {
	var contactData models.DataView
	var contactDataObjList []models.DataView
	var errList []models.DataView
	var mtx sync.Mutex
	var sanitizedUpdateRecords []*pbm.CaseUpdateEvent
	var wg sync.WaitGroup

	for _, contact := range list {
		wg.Add(1)
		go func(contact models.DmwKnownContact) {
			defer wg.Done()
			// Call DFO 3.0 GET Contacts by contactId for each record in Delta list to obtain actual metadata for contact
			contactData = makeDfoContactByIdApiCall(dfoContactByIdApiUrl, dfoAuthTokenObj, contact)

			if contactData.Err == nil {
				// Create the Update Event object from the contactData received from DFO
				event := createEvent(contactData, tenantId, busNo)

				// Using the Event Object, create the CaseUpdateEvent object
				updateMessage, err := makeCaseUpdate(event)
				if err != nil {
					contactData.Err = err
					errList = append(errList, contactData)
					return
				} else {
					updateObjSt := fmt.Sprintf("update event object created: brand: %+v, case: %+v, channel: %+v, createdAt: %+v, eventId: %vn", updateMessage.Brand, updateMessage.Case, updateMessage.Channel, updateMessage.CreatedAt, updateMessage.EventID)
					contactData.UpdateObj = updateObjSt
					contactDataObjList = append(contactDataObjList, contactData)
				}

				if updateMessage != nil && contactData.Err == nil {
					mtx.Lock()
					sanitizedUpdateRecords = append(sanitizedUpdateRecords, updateMessage)
					mtx.Unlock()
				}
			} else {
				errList = append(errList, contactData)
			}
		}(contact)
	}
	wg.Wait()

	// Range over list to print contact objects together for more readable logs
	for _, s := range contactDataObjList {
		if s.UpdateObj != "" {
			fmt.Println(s.UpdateObj)
		}
	}

	return sanitizedUpdateRecords, errList
}

func buildUri(apiType string, region string, env string) string {
	fmt.Printf("%s uri for requested region [%s] and env [%s] -- ", apiType, region, env)
	uri := ""
	switch apiType {
	case "dmwGetStates":
		uri = dmwApiUrlPrefix + region + ".omnichannel." + env + ".internal:" + dmwApiPort + dmwApiPath + dmwGetStatesEndpoint
		fmt.Println(uri)
	case "dfoAuthToken":
		switch env {
		case "prod":
			uri = dfoApiUrlPrefix + region + dfoApiV2Path + dfoAuthTokenEndpoint
			fmt.Println(uri)
		case "dev", "test", "staging":
			uri = dfoApiUrlPrefix + region + "." + env + dfoApiV2Path + dfoAuthTokenEndpoint
			fmt.Println(uri)
		default:
			break
		}
	case "dfoContactSearch":
		switch env {
		case "prod":
			uri = dfoApiUrlPrefix + region + dfoApiV3Path + dfoContactSearchEndpoint
			fmt.Println(uri)
		case "dev", "test", "staging":
			uri = dfoApiUrlPrefix + region + "." + env + dfoApiV3Path + dfoContactSearchEndpoint
			fmt.Println(uri)
		default:
			break
		}
	case "dfoContactById":
		switch env {
		case "prod":
			uri = dfoApiUrlPrefix + region + dfoApiV3Path + dfoContactByIdEndpoint
			fmt.Println(uri)
		case "dev", "test", "staging":
			uri = dfoApiUrlPrefix + region + "." + env + dfoApiV3Path + dfoContactByIdEndpoint
			fmt.Println(uri)
		default:
			break
		}
	case "dmwGrpc":
		uri = dmwGrpcUriPrefix + region + ".omnichannel." + env + ".internal:" + dmwGrpcPort
		fmt.Println(uri)
	default:
		break
	}
	return uri
}

// promptForInputData sends request for input data and validates the values, then returns the response
func promptForInputData(inputType string, requestType string) string {
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
			fmt.Println("INPUT VALUE IS NOT A VALID REGION NAME: (\"na1\", \"au1\", \"eu1\", \"jp1\", \"uk1\", \"ca1\")")
		}
	case "env":
		switch strings.ToLower(inputValue) {
		case "dev", "test", "staging", "prod":
			inputValueValid = true
		default:
			fmt.Println("INPUT VALUE IS NOT A VALID ENVIRONMENT NAME: (\"dev\", \"test\", \"staging\", \"prod\")")
		}
	case "tenantId":
		if _, err := uuid.Parse(inputValue); err != nil {
			fmt.Println("INPUT VALUE IS NOT A VALID GUID")
		} else {
			inputValueValid = true
		}
	case "clientCreds":
		if len(inputValue) != 45 {
			fmt.Println("INPUT VALUE IS NOT THE PROPER STRING LENGTH (45)")
		} else {
			inputValueValid = true
		}
	case "busNo":
		if _, err := strconv.ParseInt(inputValue, 10, 32); err != nil {
			fmt.Println("INPUT VALUE IS NOT A VALID BUSNO - SHOULD BE AN INTEGER")
		} else {
			inputValueValid = true
		}
	case "dateFrom", "dateTo":
		if inputValue != "" {
			re := regexp.MustCompile("^\\d{4}\\-(0[1-9]|1[012])\\-(0[1-9]|[12][0-9]|3[01])$") // regex for date format YYYY-mm-dd
			if re.MatchString(inputValue) {
				inputValueValid = true
			} else {
				fmt.Println("INPUT VALUE IS NOT IN THE PROPER DATE FORMAT \"YYYY-mm-dd\"")
			}
		} else {
			inputValueValid = true
		}
	case "sortType":
		switch strings.ToLower(inputValue) {
		case "asc", "desc":
			inputValueValid = true
		default:
			fmt.Println("INPUT VALUE IS NOT A VALID SORT TYPE: (\"asc\", \"desc\")")
		}
	default:
		return inputValueValid
	}
	return inputValueValid
}

// getDmwActiveContactStateData calls the Digimiddleware api POST digimiddleware/getstatesbytenants to get the list of contacts stored in DynamoDB
func getDmwActiveContactStateData(apiUrl string, tenants [1]string) (models.DmwKnownContacts, error) {
	var dmwKnownContacts models.DmwKnownContacts
	contentType := "application/json"
	method := "getDmwActiveContactStateData"
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
	} else {
		fmt.Printf(method+httpBadResponse, response.StatusCode, response.Status)
	}

	if responseData != nil {
		err = json.Unmarshal(responseData, &dmwKnownContacts)
		if err != nil {
			fmt.Printf("[%s] cannot unmarshal dmw response: [%v]\n", method, err)
			return dmwKnownContacts, err
		}
	}

	sort.SliceStable(dmwKnownContacts.Contacts, func(i, j int) bool {
		return dmwKnownContacts.Contacts[i].ContactID < dmwKnownContacts.Contacts[j].ContactID
	})

	return dmwKnownContacts, err
}

// getDfoAuthToken calls the DFO api POST engager/2.0/token to get a bearer token for subsequent DFO api calls
func getDfoAuthToken(apiUrl string, clientId string, clientSecret string) (models.DfoAuthTokenObj, error) {
	contentType := "application/json"
	var dfoAuthTokenBody models.DfoAuthTokenBody
	dfoAuthTokenBody.GrantType = "client_credentials"
	dfoAuthTokenBody.ClientId = clientId
	dfoAuthTokenBody.ClientSecret = clientSecret
	var dfoAuthTokenObj models.DfoAuthTokenObj
	method := "getDfoAuthToken"

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
	} else {
		fmt.Printf(method+httpBadResponse, response.StatusCode, response.Status)
	}

	if responseData != nil {
		err = json.Unmarshal(responseData, &dfoAuthTokenObj)
		if err != nil {
			fmt.Printf("[%s] cannot unmarshal dfo auth token: [%v]\n", method, err)
			return dfoAuthTokenObj, err
		}
	} else {
		fmt.Printf("[%s] dfo auth token was null or empty\n", method)
		return dfoAuthTokenObj, err
	}

	fmt.Printf("[%s] dfo auth token successfully retrieved\n", method)

	return dfoAuthTokenObj, err
}

// makeDfoContactSearchApiCall calls the DFO 3.0 GET Contact Search Api then loops until all data is retrieved (api only returns 25 records at a time)
func makeDfoContactSearchApiCall(dfoContactSearchApiUrl, dateFrom, dateTo, sortType string, dfoAuthTokenObj models.DfoAuthTokenObj) ([]models.DataView, error) {
	var apiUrl DfoApiUrlObj
	var dfoActiveContactList models.DfoContactSearchResponse
	var dfoData []models.DataView
	var hits int32 = 25
	method := "makeDfoContactSearchApiCall"
	var mtx sync.Mutex

	// Add URL parameters
	apiUrl.DateFrom = "&date[from]=" + dateFrom + "&"
	apiUrl.DateTo = "&date[to]=" + dateTo + "&"
	apiUrl.ScrollToken = "&scrollToken="
	apiUrl.Sorting = "&sorting=createdAt"
	apiUrl.SortingType = "&sortingType=asc"
	if sortType == "" {
		apiUrl.Url = dfoContactSearchApiUrl
	} else if dateFrom == "" {
		apiUrl.Url = dfoContactSearchApiUrl
	} else if dateTo == "" {
		apiUrl.Url = dfoContactSearchApiUrl + apiUrl.DateFrom + apiUrl.Sorting + apiUrl.SortingType
	} else {
		apiUrl.Url = dfoContactSearchApiUrl + apiUrl.DateFrom + apiUrl.DateTo + apiUrl.Sorting + apiUrl.SortingType
	}

	// Call DFO 3.0 GET Contact Search which returns 1st 25 records
	dfoResponse, err := makeDfoApiCall(apiUrl.Url, dfoAuthTokenObj, "")
	if err != nil {
		return nil, err
	}
	if dfoResponse != nil {
		err = json.Unmarshal(dfoResponse, &dfoActiveContactList)
		if err != nil {
			fmt.Printf("[%s] received dfo response but unable to unmarshal object - error: [%v]\n", method, err)
			return nil, err
		}
	}

	if dfoActiveContactList.Hits == 0 {
		fmt.Printf("[%s] returned 0 records\n", method)
		return nil, err
	}

	fmt.Printf("[%s] returned [%v] total hits\n", method, dfoActiveContactList.Hits)

	// Append first 25 Data records to Data list
	mtx.Lock()
	dfoData = append(dfoData, dfoActiveContactList.Data...)
	mtx.Unlock()

	if dfoActiveContactList.Hits > 25 {
		// Sleep between calls to not overload the GET Contact Search api
		time.Sleep(100 * time.Millisecond)

		for hits <= dfoActiveContactList.Hits {
			// Call DFO 3.0 GET Contact Search to get next 25 records
			hits += 25
			fmt.Printf("calling [%s] to get next set of records up to [%v]\n", method, hits)

			apiUrl.Url = dfoContactSearchApiUrl + apiUrl.ScrollToken + dfoActiveContactList.ScrollToken
			dfoResponse, err = makeDfoApiCall(apiUrl.Url, dfoAuthTokenObj, "")
			if err != nil {
				return nil, err
			}

			if dfoResponse != nil {
				err = json.Unmarshal(dfoResponse, &dfoActiveContactList)
				if err != nil {
					fmt.Printf("[%s] received dfo response but unable to unmarshal object - error: [%v]\n", method, err)
					return nil, err
				}

				// Append next set of Data records to Data list
				mtx.Lock()
				dfoData = append(dfoData, dfoActiveContactList.Data...)
				mtx.Unlock()
			}
		}
	}
	sort.SliceStable(dfoData, func(i, j int) bool {
		return dfoData[i].Id < dfoData[j].Id
	})

	return dfoData, nil
}

// buildDeltaList loops through the known contacts from DMW and compares them to the active contacts in DFO and creates a list of contacts that need to be updated
func buildDeltaList(dmwKnownContacts models.DmwKnownContacts, dfoData []models.DataView) models.DmwKnownContacts {
	type DmwContacts []models.DmwKnownContact
	deltaArray := DmwContacts{}
	var deltaList models.DmwKnownContacts
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
			delta := models.DmwKnownContact{
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
			deltaList = models.DmwKnownContacts{
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

// makeDfoContactByIdApiCall
func makeDfoContactByIdApiCall(dfoContactByIdApiUrl string, dfoAuthTokenObj models.DfoAuthTokenObj, contact models.DmwKnownContact) models.DataView {
	var dfoClosedContactData models.DataView

	dfoResponse, respErr := makeDfoApiCall(dfoContactByIdApiUrl, dfoAuthTokenObj, strconv.Itoa(int(contact.ContactID)))
	if respErr != nil {
		dfoClosedContactData.Err = respErr
		dfoClosedContactData.Id = strconv.FormatInt(contact.ContactID, 10)
	} else if len(dfoResponse) > 0 {
		err := json.Unmarshal(dfoResponse, &dfoClosedContactData)
		if err != nil {
			dfoClosedContactData.Err = respErr
			dfoClosedContactData.Id = strconv.FormatInt(contact.ContactID, 10)
		}
		fmt.Printf("success: received dfo data for contactId: [%d]\n", contact.ContactID)
	}

	return dfoClosedContactData
}

// makeDfoApiCall calls DFO 3.0 APIs and returns the response object
func makeDfoApiCall(apiUrl string, dfoAuthTokenObj models.DfoAuthTokenObj, contactId string) ([]byte, error) {
	var bearer = dfoAuthTokenObj.TokenType + " " + dfoAuthTokenObj.AccessToken
	method := "makeDfoApiCall"
	var responseData []byte

	if contactId != "" {
		apiUrl = apiUrl + contactId
	}

	// Create a new request using http
	req, err := http.NewRequest("GET", apiUrl, nil)

	if err != nil {
		fmt.Printf("[%s] attempt to create http.NewRequest returned an error: [%v]\n", method, err)
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
		fmt.Printf("[%s] error connecting to host\n[ERROR] - %v\n", method, err)
		return responseData, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		responseData, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("[%s] error reading response bytes: %v\n", method, err)
			return responseData, err
		}
	} else {
		err = fmt.Errorf("[%s] error calling dfo api for contact %s: %v", method, contactId, resp.Status)
		return responseData, err
	}

	return responseData, err
}

func createEvent(contactData models.DataView, tenantID, busNo string) models.StreamEventRequest {
	b, _ := strconv.ParseInt(busNo, 10, 32)
	businessUnitNo := int32(b)
	var event models.StreamEventRequest

	channelParts := strings.Split(contactData.ChannelId, "_")

	event.CreatedAt = contactData.CreatedAt
	event.EventID = uuid.New().String()
	event.EventObject = models.EventObject_Case
	event.EventType = 1
	event.Data.Brand.ID = 9999 // we don't use this so hardCode anything
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
	event.Data.Channel.Name = "channelName" // we don't use this so hardCode anything
	event.Data.Channel.IntegrationBoxIdentifier = channelParts[0]
	event.Data.Channel.IDOnExternalPlatform = channelParts[1]
	event.Data.Channel.IsPrivate = true
	event.Data.Channel.RealExternalPlatformID = channelParts[0]

	return event
}

// Turn the StreamEventRequest into a CaseUpdateEvent protobuf object to be sent via GRPC
func makeCaseUpdate(event models.StreamEventRequest) (*pbm.CaseUpdateEvent, error) {
	var err error
	method := "makeCaseUpdate"
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
		err = fmt.Errorf("[%s] non-shippable case update event - case or tenant id empty - case: %+v, tenantId: %s\n", method, message.Case, message.Brand.TenantID)
		return nil, err
	}

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
	method := "sendUpdateRecordsToMiddleware"
	if len(events.Updates) == 0 {
		fmt.Printf("[%s] no case update events were created\n", method)
		return nil
	} else {
		fmt.Printf("[%s] total count of case event updates to be sent to digimiddleware [%v]\n", method, len(events.Updates))
	}

	op := digierrors.Op("sendUpdateRecordsToMiddleware")

	// Create gRPC client to pass CaseEventUpdate to digimiddleware
	method2 := "createGrpcClient"
	t := time.Now()
	fmt.Printf("begin create grpc client: [%s]\n", method2)
	middlewareEventService := createGrpcClient(dmwGrpcApiUrl)
	fmt.Printf("[%s] - done, duration=%s\n", method2, time.Since(t))

	if middlewareEventService == nil {
		return nil
	}

	method3 := "CaseEventUpdate"
	t = time.Now()
	fmt.Printf("begin grpc call to update records in digimiddleware: [%s]\n", method3)
	response, err := middlewareEventService.CaseEventUpdate(ctx, events)
	fmt.Printf("[%s] - done, duration=%s\n", method3, time.Since(t))

	if err != nil {
		// Failure to deliver to Middleware (e.g., network errors, etc.)
		return digierrors.E(op, digierrors.IsRetryable, zapcore.ErrorLevel, err)
	}

	// If we got an error response, then the Middleware indicates this is retryable. Return an error here.
	errStr := response.GetErr()
	if errStr != "" {
		fmt.Printf("[%s] received error from case update grpc: [%v]\n", method3, errStr)
		return digierrors.E(op, digierrors.IsRetryable, zapcore.ErrorLevel, errors.New(errStr))
	}
	fmt.Printf("[%s] wrote case update records to grpc - records count: [%v]\n", method3, len(events.Updates))
	return nil
}

func createGrpcClient(dmwGrpcApiUrl string) digiservice.GrpcService {
	digimiddlewareGrpcAddr := dmwGrpcApiUrl
	conn, err := grpc.Dial(digimiddlewareGrpcAddr, grpc.WithInsecure())
	method := "createGrpcClient"
	if err != nil {
		fmt.Printf("[%s] initialization of digimiddleware grpc client failed with err: [%v]\n", method, err)
		return nil
	} else {
		fmt.Println("grpc client initialized")
	}

	middlewareEventService := digitransport.NewGRPCClient(conn, nil, nil)
	return middlewareEventService
}
