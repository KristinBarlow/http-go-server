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
	"github.com/inContact/orch-digital-middleware/pkg/digiservice"
	"github.com/inContact/orch-digital-middleware/pkg/digitransport"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io/ioutil"
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
	regionRequest       = "region - i.e. \"na1\" (Oregon), \"au1\" (Australia), \"eu1\" (Frankfurt), \"jp1\" (Japan), \"uk1\" (London), \"ca1\" (Montreal)"
	envRequest          = "environment - i.e. \"dev\", \"test\", \"staging\", \"prod\""
	clusterRequest      = "cluster - i.e. \"DO74\", \"SO32\", \"C32\""
	tenantGuidRequest   = "tenantID (in GUID format)"
	busNoRequest        = "business unit number"
	clientIdRequest     = "dfo clientId"
	clientSecretRequest = "dfo clientSecret"
	dateFromRequest     = "\"FromDate\" using format \"YYYY-mm-dd\" (OPTIONAL: Return for no date filter)"
	dateToRequest       = "\"ToDate\" in format \"YYYY-mm-dd\""
	sortTypeRequest     = "sort order - \"asc\" for ascending order or \"desc\" for descending order"

	// Log string for http responses other than 200
	httpBadResponse = " returned response other than 200 success - response.StatusCode: [%d], response.Status: [%s]\n"
)

type DfoApiUrlObj struct {
	DateFrom    string // 2022-05-01
	DateTo      string // 2022-09-15
	ScrollToken string // this is returned in dfo response if there are more than 25 hits
	Sorting     string // createdAt
	SortingType string // asc, desc
	Status      string // new, open, resolved, escalated, pending, closed, trashed
	Url         string
}

// Input variables in order they are requested
type InputDataObj struct {
	BusNo        string
	ClientId     string
	ClientSecret string
	Cluster      string
	DateFrom     string
	DateTo       string
	Env          string
	Region       string
	SortType     string
	TenantId     string
}

type TenantIdsObj struct {
	TenantIDs [1]string `json:"tenantIds"`
}

func main() {
	var dfoDataList []models.DataView
	var dfoData models.DataView
	var dmwKnownContact models.DmwKnownContacts
	var inputData InputDataObj
	var logFile []byte
	var logMsg string
	st := time.Now()
	var tenants [1]string
	var wg sync.WaitGroup

	//Prompt for needed input data
	inputData.Region = promptForInputData("region", regionRequest)
	inputData.Env = promptForInputData("env", envRequest)
	inputData.Cluster = promptForInputData("cluster", clusterRequest)
	inputData.TenantId = promptForInputData("tenantId", tenantGuidRequest)
	inputData.BusNo = promptForInputData("busNo", busNoRequest)
	inputData.ClientId = promptForInputData("clientCreds", clientIdRequest)
	inputData.ClientSecret = promptForInputData("clientCreds", clientSecretRequest)
	inputData.DateFrom = promptForInputData("dateFrom", dateFromRequest)
	if inputData.DateFrom != "" {
		inputData.DateTo = promptForInputData("dateTo", dateToRequest)
		inputData.SortType = promptForInputData("sortType", sortTypeRequest)
	}

	// Build api and gRPC URIs
	dmwContactStateApiUrl, logFile := buildUri("dmwGetStates", inputData, logFile)
	dfoAuthTokenApiUrl, logFile := buildUri("dfoAuthToken", inputData, logFile)
	dfoContactSearchApiUrl, logFile := buildUri("dfoContactSearch", inputData, logFile)
	dfoContactByIdApiUrl, logFile := buildUri("dfoContactById", inputData, logFile)
	dmwGrpcApiUrl, logFile := buildUri("dmwGrpc", inputData, logFile)

	// Get DFO auth token
	var dfoAuthTokenObj models.DfoAuthTokenObj
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		method := "getDfoAuthToken"
		t := time.Now()

		logMsg = fmt.Sprintf("begin api call: getDfoAuthToken\n")
		logFile = createLog(logMsg, logFile)

		dfoAuthTokenObj, logFile, err = getDfoAuthToken(dfoAuthTokenApiUrl, inputData, logFile)
		if err != nil {
			dfoAuthTokenObj.Error = err

			logMsg = fmt.Sprintf("error calling [%s]: [%v]\n", method, err)
			logFile = createLog(logMsg, logFile)

			return
		}
		logMsg = fmt.Sprintf("[%s] - done, duration=%s\n", method, time.Since(t))
		logFile = createLog(logMsg, logFile)
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
			tenants[0] = inputData.TenantId

			logMsg = fmt.Sprintf("begin api call: [%s]\n", method)
			logFile = createLog(logMsg, logFile)

			dmwKnownContact, logFile, err = getDmwActiveContactStateData(dmwContactStateApiUrl, tenants, logFile)
			if err != nil {
				dmwKnownContact.Error = err

				logMsg = fmt.Sprintf("error calling [%s]: [%v]\n", method, err)
				logFile = createLog(logMsg, logFile)
				return
			}
			logMsg = fmt.Sprintf("[%s] - done, duration=%s, returned [%d] total contacts\n", method, time.Since(t), len(dmwKnownContact.Contacts))
			logFile = createLog(logMsg, logFile)
		}()
		wg.Wait()

		// Call DFO 3.0 GET Contact Search API to get list of DFO active contacts
		var err error
		wg.Add(1)
		go func() {
			defer wg.Done()
			method := "makeDfoContactSearchApiCall"
			t := time.Now()

			logMsg = fmt.Sprintf("begin api call: [%s]\n", method)
			logFile = createLog(logMsg, logFile)

			dfoDataList, logFile, err = makeDfoContactSearchApiCall(dfoContactSearchApiUrl, inputData, dfoAuthTokenObj, logFile)
			if err != nil {
				dfoData.Err = err

				logMsg = fmt.Sprintf("error calling [%s]]: [%v]\n", method, err)
				logFile = createLog(logMsg, logFile)

				return
			}
			logMsg = fmt.Sprintf("[%s] - done, duration=%s, dfoActiveContacts=%d\n", method, time.Since(t), len(dfoDataList))
			logFile = createLog(logMsg, logFile)
		}()
		wg.Wait()
	}

	// Compare lists to get the contacts that exist in DMW but are closed in DFO.
	var deltaContacts models.DmwKnownContacts
	method := "buildDeltaList"

	if dmwKnownContact.Error == nil && dfoData.Err == nil {
		if len(dmwKnownContact.Contacts) > 0 {
			logMsg = fmt.Sprintf("begin building list: [%s]\n", method)
			logFile = createLog(logMsg, logFile)

			t := time.Now()
			deltaContacts, logFile = buildDeltaList(inputData, dmwKnownContact, dfoDataList, logFile)

			logMsg = fmt.Sprintf("[%s] - done, duration=%s, deltaContacts=%d\n", method, time.Since(t), len(deltaContacts.Contacts))
			logFile = createLog(logMsg, logFile)

		} else {
			logMsg = fmt.Sprintf("dmw list was empty due to no contacts or error retrieving data from api - will not attempt to process updates")
			logFile = createLog(logMsg, logFile)

			return
		}
	} else {
		logMsg = fmt.Sprintf("error retrieving data from api - will not attempt to process updates")
		logFile = createLog(logMsg, logFile)

		return
	}

	// Batch and Process records to digimiddleware using gRPC
	if len(deltaContacts.Contacts) > 0 {
		logFile = process(deltaContacts.Contacts, dfoAuthTokenObj, dfoContactByIdApiUrl, dmwGrpcApiUrl, inputData, logFile)
	} else {
		logMsg = fmt.Sprintf("comparison of lists returned 0 contacts to update - will not attempt to process updates")
		logFile = createLog(logMsg, logFile)

		return
	}

	logMsg = fmt.Sprintf("update case state service completed - totalDuration = %s\n", time.Since(st))
	logFile = createLog(logMsg, logFile)

	// Create output logFile file
	filepath := fmt.Sprintf("C:\\Users\\kristin.barlow\\ContactCloseUpdates\\Logs\\LOG_%s_%s_%d", inputData.Cluster, inputData.BusNo, time.Now().UnixNano())

	writeLogFile(".csv", filepath, logFile)
}

// process batches the data into specified batchSize to process
func process(data []models.DmwKnownContact, dfoAuthTokenObj models.DfoAuthTokenObj, dfoContactByIdApiUrl, dmwGrpcApiUrl string, inputData InputDataObj, log []byte) []byte {
	batchCount := 1
	var errList []models.DataView
	var logMsg string
	method := "processBatch"
	method2 := "sendUpdateRecordsToMiddleware"
	var sanitizedUpdateRecords []*pbm.CaseUpdateEvent

	for start, end := 0, 0; start <= len(data)-1; start = end {
		var err error

		end = start + batchSize
		if end > len(data) {
			end = len(data)
		}
		batch := data[start:end]

		t := time.Now()

		logMsg = fmt.Sprintf("begin processing batch [%d]\n", batchCount)
		log = createLog(logMsg, log)

		sanitizedUpdateRecords, log, errList = processBatch(batch, dfoContactByIdApiUrl, dfoAuthTokenObj, inputData, log)

		logMsg = fmt.Sprintf("[%s] [%d] - done, duration=%s, total records to update=%d\n", method, batchCount, time.Since(t), len(sanitizedUpdateRecords))
		log = createLog(logMsg, log)

		if errList != nil {
			logMsg = fmt.Sprintf("ERROR processing batch - will not attempt to update below [%d] contacts\n", len(errList))
			log = createLog(logMsg, log)

			// Range over errList to print all errors together for more readable logs
			for _, e := range errList {
				logMsg = fmt.Sprintln(e.Err)
				log = createLog(logMsg, log)
			}
		}
		batchCount++

		// Push sanitizedUpdateRecords to digimiddleware via gRPC
		if sanitizedUpdateRecords != nil {

			logMsg = fmt.Sprintf("begin pushing updates to digimiddleware: [%s]\n", method2)
			log = createLog(logMsg, log)

			t := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			log, err = sendUpdateRecordsToMiddleware(ctx, &pbm.CaseUpdateEvents{
				Updates:    sanitizedUpdateRecords,
				ReceivedAt: timestamppb.Now(),
			}, dmwGrpcApiUrl, log)

			if err != nil {
				logMsg = fmt.Sprintf("[%s] error making grpc call to send update records to middleware: [%v]\n", method2, err)
				log = createLog(logMsg, log)

				cancel()
				return log
			}
			cancel()
			logMsg = fmt.Sprintf("[%s] - done, duration=%s, count=%d\n", method2, time.Since(t), len(sanitizedUpdateRecords))
			log = createLog(logMsg, log)
		} else {
			logMsg = fmt.Sprintf("[%s] there were no contacts added to sanitizedUpdateRecords list - will not attempt to process updates\n", method)
			log = createLog(logMsg, log)
		}
	}

	// Create output file and print updated contacts to console
	filepath := fmt.Sprintf("C:\\Users\\kristin.barlow\\ContactCloseUpdates\\%s_%s_%d", inputData.Cluster, inputData.BusNo, time.Now().UnixNano())

	mr, _ := json.Marshal(sanitizedUpdateRecords)

	writeLogFile(".csv", filepath, mr)

	for _, record := range sanitizedUpdateRecords {
		logMsg = fmt.Sprintln(record)
		log = createLog(logMsg, log)
	}

	return log
}

// processBatch calls DFO 3.0 GET Contact to obtain contact data to build the case update event
func processBatch(list []models.DmwKnownContact, dfoContactByIdApiUrl string, dfoAuthTokenObj models.DfoAuthTokenObj, i InputDataObj, log []byte) ([]*pbm.CaseUpdateEvent, []byte, []models.DataView) {
	var errList []models.DataView
	var sanitizedUpdateRecords []*pbm.CaseUpdateEvent
	var wg sync.WaitGroup

	for _, contact := range list {
		wg.Add(1)
		go func(contact models.DmwKnownContact, errList []models.DataView) {
			defer wg.Done()
			var contactData models.DataView
			var l []byte
			var mtx sync.Mutex
			// Call DFO 3.0 GET Contacts by contactId for each record in Delta list to obtain actual metadata for contact
			contactData, l = makeDfoContactByIdApiCall(dfoContactByIdApiUrl, dfoAuthTokenObj, contact, l)

			if contactData.Err == nil {
				// Create the Update Event object from the contactData received from DFO
				event := createEvent(contactData, i.TenantId, i.BusNo)

				// Using the Event Object, create the CaseUpdateEvent object
				updateMessage, err := makeCaseUpdate(event)
				if err != nil {
					contactData.Err = err
					mtx.Lock()
					errList = append(errList, contactData)
					mtx.Unlock()
					return
				}

				if updateMessage != nil && contactData.Err == nil {
					mtx.Lock()
					sanitizedUpdateRecords = append(sanitizedUpdateRecords, updateMessage)
					log = append(log, l...)
					mtx.Unlock()
				}
			} else {
				mtx.Lock()
				errList = append(errList, contactData)
				mtx.Unlock()
			}
		}(contact, errList)
	}
	wg.Wait()

	return sanitizedUpdateRecords, log, errList
}

func buildUri(apiType string, i InputDataObj, log []byte) (string, []byte) {
	uri := ""

	logMsg := fmt.Sprintf("%s uri for requested region [%s] and env [%s] -- ", apiType, i.Region, i.Env)
	log = createLog(logMsg, log)

	switch apiType {
	case "dmwGetStates":
		uri = dmwApiUrlPrefix + i.Region + ".omnichannel." + i.Env + ".internal:" + dmwApiPort + dmwApiPath + dmwGetStatesEndpoint
		logMsg = fmt.Sprintln(uri)
		log = createLog(logMsg, log)
	case "dfoAuthToken":
		switch i.Env {
		case "prod":
			uri = dfoApiUrlPrefix + i.Region + dfoApiV2Path + dfoAuthTokenEndpoint
			logMsg = fmt.Sprintln(uri)
			log = createLog(logMsg, log)
		case "dev", "test", "staging":
			uri = dfoApiUrlPrefix + i.Region + "." + i.Env + dfoApiV2Path + dfoAuthTokenEndpoint
			logMsg = fmt.Sprintln(uri)
			log = createLog(logMsg, log)
		default:
			break
		}
	case "dfoContactSearch":
		switch i.Env {
		case "prod":
			uri = dfoApiUrlPrefix + i.Region + dfoApiV3Path + dfoContactSearchEndpoint
			logMsg = fmt.Sprintln(uri)
			log = createLog(logMsg, log)
		case "dev", "test", "staging":
			uri = dfoApiUrlPrefix + i.Region + "." + i.Env + dfoApiV3Path + dfoContactSearchEndpoint
			logMsg = fmt.Sprintln(uri)
			log = createLog(logMsg, log)
		default:
			break
		}
	case "dfoContactById":
		switch i.Env {
		case "prod":
			uri = dfoApiUrlPrefix + i.Region + dfoApiV3Path + dfoContactByIdEndpoint
			logMsg = fmt.Sprintln(uri)
			log = createLog(logMsg, log)
		case "dev", "test", "staging":
			uri = dfoApiUrlPrefix + i.Region + "." + i.Env + dfoApiV3Path + dfoContactByIdEndpoint
			logMsg = fmt.Sprintln(uri)
			log = createLog(logMsg, log)
		default:
			break
		}
	case "dmwGrpc":
		uri = dmwGrpcUriPrefix + i.Region + ".omnichannel." + i.Env + ".internal:" + dmwGrpcPort
		logMsg = fmt.Sprintln(uri)
		log = createLog(logMsg, log)
	default:
		break
	}
	return uri, log
}

// promptForInputData sends request for input data and validates the values, then returns the response
func promptForInputData(inputType string, requestType string) string {
	response := getInputData(requestType)

	validInputData := false
	for !validInputData {
		validInputData = validateResponse(response, inputType)
		if !validInputData {
			response = getInputData(requestType)
		}
	}
	return response
}

// getInputData requests user input and returns value
func getInputData(inputType string) string {
	fmt.Printf("input %v: ", inputType)
	reader := bufio.NewReader(os.Stdin)

	response, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println(err)
	}

	return strings.TrimSuffix(response, "\n")
}

// validateResponse verifies that the data input by the user was in a proper type or format
func validateResponse(inputValue string, inputType string) bool {
	switch inputType {
	case "region":
		switch strings.ToLower(inputValue) {
		case "na1", "au1", "eu1", "jp1", "uk1", "ca1":
			return true
		default:
			fmt.Println("INPUT VALUE IS NOT A VALID REGION NAME: (\"na1\", \"au1\", \"eu1\", \"jp1\", \"uk1\", \"ca1\")")
			return false
		}
	case "env":
		switch strings.ToLower(inputValue) {
		case "dev", "test", "staging", "prod":
			return true
		default:
			fmt.Println("INPUT VALUE IS NOT A VALID ENVIRONMENT NAME: (\"dev\", \"test\", \"staging\", \"prod\")")
			return false
		}
	case "cluster":
		if len(inputValue) > 2 || len(inputValue) < 5 {
			return true
		} else {
			fmt.Println("INPUT VALUE IS NOT THE PROPER STRING LENGTH")
			return false
		}
	case "tenantId":
		if _, err := uuid.Parse(inputValue); err != nil {
			fmt.Println("INPUT VALUE IS NOT A VALID GUID")
			return false
		} else {
			return true
		}
	case "clientCreds":
		if len(inputValue) != 45 {
			fmt.Println("INPUT VALUE IS NOT THE PROPER STRING LENGTH (45)")
			return false
		} else {
			return true
		}
	case "busNo":
		if _, err := strconv.ParseInt(inputValue, 10, 32); err != nil {
			fmt.Println("INPUT VALUE IS NOT A VALID BUSNO - SHOULD BE AN INTEGER")
			return false
		} else {
			return true
		}
	case "dateFrom", "dateTo":
		if inputValue != "" {
			re := regexp.MustCompile("^\\d{4}\\-(0[1-9]|1[012])\\-(0[1-9]|[12][0-9]|3[01])$") // regex for date format YYYY-mm-dd
			if re.MatchString(inputValue) {
				return true
			} else {
				fmt.Println("INPUT VALUE IS NOT IN THE PROPER DATE FORMAT \"YYYY-mm-dd\"")
				return false
			}
		} else {
			return true
		}
	case "sortType":
		switch strings.ToLower(inputValue) {
		case "asc", "desc":
			return true
		default:
			fmt.Println("INPUT VALUE IS NOT A VALID SORT TYPE: (\"asc\", \"desc\")")
			return false
		}
	default:
		return false
	}
}

// getDfoAuthToken calls the DFO api POST engager/2.0/token to get a bearer token for subsequent DFO api calls
func getDfoAuthToken(apiUrl string, i InputDataObj, log []byte) (models.DfoAuthTokenObj, []byte, error) {
	contentType := "application/json"
	var dfoAuthTokenBody models.DfoAuthTokenBody
	dfoAuthTokenBody.GrantType = "client_credentials"
	dfoAuthTokenBody.ClientId = i.ClientId
	dfoAuthTokenBody.ClientSecret = i.ClientSecret
	var dfoAuthTokenObj models.DfoAuthTokenObj
	var logMsg string
	method := "getDfoAuthToken"

	bodyJson, _ := json.Marshal(dfoAuthTokenBody)
	reader := bytes.NewReader(bodyJson)
	var responseData []byte

	response, err := http.Post(apiUrl, contentType, reader)

	if err != nil {
		logMsg = fmt.Sprintf(err.Error())
		log = createLog(logMsg, log)

		return dfoAuthTokenObj, log, err
	}

	if response.StatusCode == 200 {
		responseData, err = ioutil.ReadAll(response.Body)
		if err != nil {
			logMsg = fmt.Sprintf(err.Error())
			log = createLog(logMsg, log)

			return dfoAuthTokenObj, log, err
		}
	} else {
		fmt.Printf(method+httpBadResponse, response.StatusCode, response.Status)
	}

	if responseData != nil {
		err = json.Unmarshal(responseData, &dfoAuthTokenObj)
		if err != nil {
			logMsg = fmt.Sprintf("[%s] cannot unmarshal dfo auth token: [%v]\n", method, err)
			log = createLog(logMsg, log)

			return dfoAuthTokenObj, log, err
		}
	} else {
		logMsg = fmt.Sprintf("[%s] dfo auth token was null or empty\n", method)
		log = createLog(logMsg, log)

		return dfoAuthTokenObj, log, err
	}

	logMsg = fmt.Sprintf("[%s] dfo auth token successfully retrieved\n", method)
	log = createLog(logMsg, log)

	return dfoAuthTokenObj, log, err
}

// getDmwActiveContactStateData calls the Digimiddleware api POST digimiddleware/getstatesbytenants to get the list of contacts stored in DynamoDB
func getDmwActiveContactStateData(apiUrl string, tenants [1]string, log []byte) (models.DmwKnownContacts, []byte, error) {
	var dmwKnownContact models.DmwKnownContacts
	contentType := "application/json"
	var logMsg string
	method := "getDmwActiveContactStateData"
	var tenantIdsObj TenantIdsObj
	tenantIdsObj.TenantIDs = tenants

	bodyJson, _ := json.Marshal(tenantIdsObj)
	reader := bytes.NewReader(bodyJson)
	var responseData []byte

	response, err := http.Post(apiUrl, contentType, reader)

	if err != nil {
		logMsg = err.Error()
		log = createLog(logMsg, log)

		return dmwKnownContact, log, err
	}

	if response.StatusCode == 200 {
		responseData, err = ioutil.ReadAll(response.Body)
		if err != nil {
			logMsg = err.Error()
			log = createLog(logMsg, log)

			return dmwKnownContact, log, err
		}
	} else {
		fmt.Printf(method+httpBadResponse, response.StatusCode, response.Status)
	}

	if responseData != nil {
		err = json.Unmarshal(responseData, &dmwKnownContact)
		if err != nil {
			logMsg = fmt.Sprintf("[%s] cannot unmarshal dmw response: [%v]\n", method, err)
			log = createLog(logMsg, log)

			return dmwKnownContact, log, err
		}
	}

	sort.SliceStable(dmwKnownContact.Contacts, func(i, j int) bool {
		return dmwKnownContact.Contacts[i].ContactID < dmwKnownContact.Contacts[j].ContactID
	})

	return dmwKnownContact, log, err
}

// makeDfoContactSearchApiCall calls the DFO 3.0 GET Contact Search Api then loops until all data is retrieved (api only returns 25 records at a time)
func makeDfoContactSearchApiCall(dfoContactSearchApiUrl string, i InputDataObj, dfoAuthTokenObj models.DfoAuthTokenObj, log []byte) ([]models.DataView, []byte, error) {
	var apiUrl DfoApiUrlObj
	var dfoActiveContactList models.DfoContactSearchResponse
	var dfoData []models.DataView
	var hits int32 = 25
	var logMsg string
	method := "makeDfoContactSearchApiCall"
	var mtx sync.Mutex

	// Add URL parameters
	apiUrl.DateFrom = "&date[from]=" + i.DateFrom + "&"
	apiUrl.DateTo = "&date[to]=" + i.DateTo + "&"
	apiUrl.ScrollToken = "&scrollToken="
	apiUrl.Sorting = "&sorting=createdAt"
	apiUrl.SortingType = "&sortingType=asc"
	if i.SortType == "" {
		apiUrl.Url = dfoContactSearchApiUrl
	} else if i.DateFrom == "" {
		apiUrl.Url = dfoContactSearchApiUrl
	} else if i.DateTo == "" {
		apiUrl.Url = dfoContactSearchApiUrl + apiUrl.DateFrom + apiUrl.Sorting + apiUrl.SortingType
	} else {
		apiUrl.Url = dfoContactSearchApiUrl + apiUrl.DateFrom + apiUrl.DateTo + apiUrl.Sorting + apiUrl.SortingType
	}

	// Call DFO 3.0 GET Contact Search which returns 1st 25 records
	dfoResponse, err := makeDfoApiCall(apiUrl.Url, dfoAuthTokenObj, "")
	if err != nil {
		return nil, log, err
	}
	if dfoResponse != nil {
		err = json.Unmarshal(dfoResponse, &dfoActiveContactList)
		if err != nil {
			logMsg = fmt.Sprintf("[%s] received dfo response but unable to unmarshal object - error: [%v]\n", method, err)
			log = createLog(logMsg, log)

			return nil, log, err
		}
	}

	if dfoActiveContactList.Hits == 0 {
		logMsg = fmt.Sprintf("[%s] returned 0 records\n", method)
		log = createLog(logMsg, log)

		return nil, log, err
	}

	logMsg = fmt.Sprintf("[%s] returned [%v] total hits\n", method, dfoActiveContactList.Hits)
	log = createLog(logMsg, log)

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

			logMsg = fmt.Sprintf("calling [%s] to get next set of records up to [%v]\n", method, hits)
			log = createLog(logMsg, log)

			apiUrl.Url = dfoContactSearchApiUrl + apiUrl.ScrollToken + dfoActiveContactList.ScrollToken
			dfoResponse, err = makeDfoApiCall(apiUrl.Url, dfoAuthTokenObj, "")
			if err != nil {
				return nil, log, err
			}

			if dfoResponse != nil {
				err = json.Unmarshal(dfoResponse, &dfoActiveContactList)
				if err != nil {
					logMsg = fmt.Sprintf("[%s] received dfo response but unable to unmarshal object - error: [%v]\n", method, err)
					log = createLog(logMsg, log)

					return nil, log, err
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

	return dfoData, log, nil
}

// buildDeltaList loops through the known contacts from DMW and compares them to the active contacts in DFO and creates a list of contacts that need to be updated
func buildDeltaList(i InputDataObj, dmwKnownContact models.DmwKnownContacts, dfoData []models.DataView, log []byte) (models.DmwKnownContacts, []byte) {
	type DmwContacts []models.DmwKnownContact
	deltaArray := DmwContacts{}
	var deltaList models.DmwKnownContacts
	foundCount := 0
	var logMsg string
	var mtx sync.Mutex
	notFoundCount := 0
	alreadyClosedCount := 0

	// Loop through DmwKnownContact.Contacts and check each contact data in DfoActiveContacts to see if we find a match
	for _, contact := range dmwKnownContact.Contacts {
		found := false
		shouldClose := false

		// Only compare contact with DFO data if contact is not closed (18)
		if contact.CurrentContactState != 18 {
			// Compare the data from DFO with the DMW data
			for _, d := range dfoData {
				dataId, _ := strconv.ParseInt(d.Id, 10, 64)
				shouldClose = false
				matchesDateFilter := true

				// If date filter was added, only check dmw known contacts between those dates
				if i.DateFrom != "" && i.DateTo != "" {
					if contact.CurrentContactDate <= i.DateFrom || contact.CurrentContactDate >= i.DateTo {
						matchesDateFilter = false
					}
				}

				if !matchesDateFilter {
					break
				}

				if contact.ContactID == dataId {
					found = true
					foundCount++
					break
				} else {
					shouldClose = true
				}
			}

			if !found && shouldClose {
				notFoundCount++

				logMsg = fmt.Sprintf("ContactID*[%d]*Found*[%v]*ShouldClose*[%v]*DmwContactState*[%d]*CurrentContactDate*[%v]\n", contact.ContactID, found, shouldClose, contact.CurrentContactState, contact.CurrentContactDate)
				log = createLog(logMsg, log)
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

			mtx.Lock()
			deltaArray = append(deltaArray, delta)
			mtx.Unlock()

			deltaList = models.DmwKnownContacts{
				Contacts: deltaArray,
			}
		}
	}

	logMsg = fmt.Sprintf("total dmw known contacts: %d\n", len(dmwKnownContact.Contacts))
	log = createLog(logMsg, log)
	logMsg = fmt.Sprintf("total contacts that were already closed (will not attempt to update): %d\n", alreadyClosedCount)
	log = createLog(logMsg, log)
	logMsg = fmt.Sprintf("total contacts that were not found (will attempt to update): %d\n", notFoundCount)
	log = createLog(logMsg, log)
	logMsg = fmt.Sprintf("total contacts that were found (will not attempt to update): %d\n", foundCount)
	log = createLog(logMsg, log)

	return deltaList, log
}

// makeDfoContactByIdApiCall calls makeDfoApiCall to get each contact data by contact id
func makeDfoContactByIdApiCall(dfoContactByIdApiUrl string, dfoAuthTokenObj models.DfoAuthTokenObj, contact models.DmwKnownContact, log []byte) (models.DataView, []byte) {
	var dfoClosedContactData models.DataView
	var logMsg string

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

		logMsg = fmt.Sprintf("success: received dfo data for contactId: [%d]\n", contact.ContactID)
		log = createLog(logMsg, []byte(logMsg))
	}

	return dfoClosedContactData, log
}

// makeDfoApiCall calls DFO 3.0 APIs and returns the response object
func makeDfoApiCall(apiUrl string, dfoAuthTokenObj models.DfoAuthTokenObj, contactId string) ([]byte, error) {
	var bearer = dfoAuthTokenObj.TokenType + " " + dfoAuthTokenObj.AccessToken
	client := &http.Client{}
	method := "makeDfoApiCall"
	resp := &http.Response{}
	var responseData []byte

	if contactId != "" {
		apiUrl = apiUrl + contactId
	}

	// Create a new request using http
	req, err := http.NewRequest("GET", apiUrl, nil)

	if err != nil {
		err = fmt.Errorf("[%s] attempt to create http.NewRequest returned an error: [%v]\n", method, err)
		return responseData, err
	}
	if req != nil {
		// add authorization header and content-type to the req
		req.Header.Add("Authorization", bearer)
		req.Header.Add("Content-Type", "application/json")
	}

	// Send req using http Client
	resp, err = client.Do(req)
	if err != nil {
		err = fmt.Errorf("[%s] error connecting to host\n[ERROR] - %v\n", method, err)
		return responseData, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		responseData, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			err = fmt.Errorf("[%s] error reading response bytes: %v\n", method, err)
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
	event.Data.Channel.Name = ""
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

func sendUpdateRecordsToMiddleware(ctx context.Context, events *pbm.CaseUpdateEvents, dmwGrpcApiUrl string, log []byte) ([]byte, error) {
	var logMsg string
	method := "sendUpdateRecordsToMiddleware"

	if len(events.Updates) == 0 {
		logMsg = fmt.Sprintf("[%s] no case update events were created\n", method)
		log = createLog(logMsg, log)

		return log, nil
	} else {
		logMsg = fmt.Sprintf("[%s] total count of case event updates to be sent to digimiddleware [%v]\n", method, len(events.Updates))
		log = createLog(logMsg, log)
	}

	// Create gRPC client to pass CaseEventUpdate to digimiddleware
	method2 := "createGrpcClient"
	t := time.Now()

	logMsg = fmt.Sprintf("begin create grpc client: [%s]\n", method2)
	log = createLog(logMsg, log)

	middlewareEventService, log := createGrpcClient(dmwGrpcApiUrl, log)

	logMsg = fmt.Sprintf("[%s] - done, duration=%s\n", method2, time.Since(t))
	log = createLog(logMsg, log)

	if middlewareEventService == nil {
		return log, nil
	}

	method3 := "CaseEventUpdate"
	t = time.Now()

	logMsg = fmt.Sprintf("begin grpc call to update records in digimiddleware: [%s]\n", method3)
	log = createLog(logMsg, log)

	//response, err := middlewareEventService.CaseEventUpdate(ctx, events)
	logMsg = fmt.Sprintf("[%s] - done, duration=%s\n", method3, time.Since(t))
	log = createLog(logMsg, log)

	//if err != nil {
	//	// Failure to deliver to Middleware (e.g., network errors, etc.)
	//	logMsg = fmt.Sprintln(err.Error())
	//  log = createLog(logMsg, log)
	//}

	// If we got an error response, then the Middleware indicates this is retryable. Return an error here.
	//errStr := response.GetErr()
	//if errStr != "" {
	//	logMsg = fmt.Sprintf("[%s] received error from case update grpc: [%v]\n", method3, errStr)
	//  log = createLog(logMsg, log)
	//
	//	return log, nil
	//}

	logMsg = fmt.Sprintf("[%s] wrote case update records to grpc - records count: [%v]\n", method3, len(events.Updates))
	log = createLog(logMsg, log)

	return log, nil
}

func createGrpcClient(dmwGrpcApiUrl string, log []byte) (digiservice.GrpcService, []byte) {
	digimiddlewareGrpcAddr := dmwGrpcApiUrl
	var logMsg string

	conn, err := grpc.Dial(digimiddlewareGrpcAddr, grpc.WithInsecure())
	method := "createGrpcClient"
	if err != nil {
		logMsg = fmt.Sprintf("[%s] initialization of digimiddleware grpc client failed with err: [%v]\n", method, err)
		log = createLog(logMsg, log)

		return nil, log
	} else {
		logMsg = fmt.Sprintf("grpc client initialized\n")
		log = createLog(logMsg, log)
	}

	middlewareEventService := digitransport.NewGRPCClient(conn, nil, nil)

	return middlewareEventService, log
}

// createLog prints message to console and appends log message to log file
func createLog(msg string, log []byte) []byte {
	fmt.Printf(msg)
	log = appendToFile(log, msg)

	return log
}

// appendToFile appends a message to a byte array file
func appendToFile(file []byte, msg string) []byte {
	var mtx sync.Mutex

	mtx.Lock()
	file = append(file, []byte(msg)...)
	mtx.Unlock()

	return file
}

// writeLogFile is a helper function to output a log file to path
func writeLogFile(fileMode, filepath string, file []byte) {
	filepath = filepath + fileMode

	err := os.WriteFile(filepath, file, 0644)
	if err != nil {
		// If error writing log files, print error but no need to add to log
		fmt.Printf("There was an error writing logs to %s - error: %s", filepath, err)
	}
}
