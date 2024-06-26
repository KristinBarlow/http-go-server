package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"github.com/MyGoProjects/http-go-server/src/models"
	"github.com/google/uuid"
	"github.com/inContact/orch-common/fsm"
	pbm "github.com/inContact/orch-common/proto/digi/digimiddleware"
	"github.com/inContact/orch-digital-middleware/pkg/digiservice"
	"github.com/inContact/orch-digital-middleware/pkg/digitransport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
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
	batchSize   = 100
	contentType = "application/json"

	// Function names for logging
	buildDeltaListOp                = "buildDeltaList"
	caseEventUpdateOp               = "caseEventUpdate"
	createGrpcClientOp              = "createGrpcClient"
	getChannelsOp                   = "getChannels"
	getDfoAuthTokenOp               = "getDfoAuthToken"
	getDmwActiveContactStateDataOp  = "getDmwActiveContactStateData"
	getServiceTokenOp               = "getServiceToken"
	getTenantDataOp                 = "getTenantData"
	makeCaseUpdateOp                = "makeCaseUpdate"
	makeCxOneApiCallOp              = "makeCxOneApiCall"
	makeDfoApiCallOp                = "makeDfoApiCall"
	makeDfoContactSearchApiCallOp   = "makeDfoContactSearchApiCall"
	processBatchOp                  = "processBatch"
	processNotFoundEventOp          = "processNotFoundEvent"
	sendUpdateRecordsToMiddlewareOp = "sendUpdateRecordsToMiddleware"

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

type ApiObjects struct {
	CxOneApi CxOneApiObj
	DfoApi   DfoApiObj
	DmwApi   DmwApiObj
	DmwGrpc  DmwGrpcObj
}
type CxOneApiObj struct {
	UrlPrefix             string
	Path                  string
	PathNoDash            string
	GetTenantByIdEndpoint string
	GetTokenEndpoint      string
}

type DfoApiObj struct {
	UrlPrefix                         string
	V2Path                            string
	V3Path                            string
	AuthTokenEndpoint                 string
	GetChannelsEndpoint               string
	GetContactSearchEndpoint          string
	ContactSearchActiveContactsFilter string
	GetContactByIdEndpoint            string
}

type DmwApiObj struct {
	UrlPrefix         string
	Path              string
	Port              string
	GetStatesEndpoint string
}

type DmwGrpcObj struct {
	UriPrefix string
	Port      string
}

type InputRequestObj struct {
	Region               string
	Environment          string
	TenantId             string
	CxOneAccessKeyId     string
	CxOneAccessKeySecret string
	DfoClientId          string
	DfoClientSecret      string
	DateFrom             string
	DateTo               string
}

type InputDataObj struct {
	Region          string
	Env             string
	TenantId        string
	AccessKeyId     string
	AccessKeySecret string
	ClientId        string
	ClientSecret    string
	DateFrom        string
	DateTo          string
}

type TenantIdsWrapper struct {
	TenantIDs [1]string `json:"tenantIds"`
}

// Define a map of environment names to CA certificate paths
var certPaths = map[string]string{
	"prod":    "src/certs/lax-incer01.inucn.com.cer",
	"dev":     "src/certs/in-ena-incer01-ca.cer",
	"test":    "src/certs/in-ena-incer01-ca.cer",
	"staging": "src/certs/in-ena-incer01-ca.cer",
	"perf":    "src/certs/in-ena-incer01-ca.cer",
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

	var cxOneApi = CxOneApiObj{
		UrlPrefix:             "https://",
		Path:                  ".nice-incontact.com/",
		PathNoDash:            ".niceincontact.com/",
		GetTenantByIdEndpoint: "tenants/id/",
		GetTokenEndpoint:      "authentication/v1/token/access-key",
	}

	var dfoApi = DfoApiObj{
		UrlPrefix:                         "https://api-de-",
		V2Path:                            ".niceincontact.com/engager/2.0",
		V3Path:                            ".niceincontact.com/dfo/3.0",
		AuthTokenEndpoint:                 "/token",
		GetChannelsEndpoint:               "/channels",
		GetContactSearchEndpoint:          "/contacts",
		ContactSearchActiveContactsFilter: "?status[]=new&status[]=resolved&status[]=escalated&status[]=pending&status[]=open",
		GetContactByIdEndpoint:            "/contacts/",
	}

	var dmwApi = DmwApiObj{ //old Eks
		UrlPrefix:         "http://digi-shared-eks01-",
		Path:              "/digimiddleware",
		Port:              "8085",
		GetStatesEndpoint: "/getstatesbytenants",
	}

	var dmwGrpc = DmwGrpcObj{ //old Eks
		UriPrefix: "digi-shared-eks01-",
		Port:      "9884",
	}

	var apiObjects = ApiObjects{
		CxOneApi: cxOneApi,
		DfoApi:   dfoApi,
		DmwApi:   dmwApi,
		DmwGrpc:  dmwGrpc,
	}

	var inputRequest = InputRequestObj{
		Region:               "region - i.e. \"na1\" (Oregon), \"na2\" (FedRAMP)\", \"au1\" (Australia), \"eu1\" (Frankfurt), \"jp1\" (Japan), \"uk1\" (London), \"ca1\" (Montreal), \"ae1\" (UAE)",
		Environment:          "environment - i.e. \"dev\", \"test\", \"staging\", \"prod\"",
		TenantId:             "tenantID (in GUID format)",
		CxOneAccessKeyId:     "cxone accessKeyId (decode k9s - :secrets - digimiddleware-mcr-access-details)",
		CxOneAccessKeySecret: "cxone accessKeySecret (decode k9s - :secrets - digimiddleware-mcr-access-details)",
		DfoClientId:          "dfo clientId",
		DfoClientSecret:      "dfo clientSecret",
		DateFrom:             "\"FromDate\" using format \"YYYY-mm-dd\" (OPTIONAL: Return for no date filter)",
		DateTo:               "\"ToDate\" in format \"YYYY-mm-dd\"",
	}

	//Prompt for needed input data
	inputData.Region = promptForInputData("region", inputRequest.Region)
	inputData.Region = strings.ToLower(inputData.Region)
	inputData.Env = promptForInputData("env", inputRequest.Environment)
	inputData.Env = strings.ToLower(inputData.Env)
	inputData.AccessKeyId = promptForInputData("accessKeyCreds", inputRequest.CxOneAccessKeyId)
	inputData.AccessKeySecret = promptForInputData("accessKeyCreds", inputRequest.CxOneAccessKeySecret)
	inputData.TenantId = promptForInputData("tenantId", inputRequest.TenantId)
	inputData.ClientId = promptForInputData("clientCreds", inputRequest.DfoClientId)
	inputData.ClientSecret = promptForInputData("clientCreds", inputRequest.DfoClientSecret)
	inputData.DateFrom = promptForInputData("dateFrom", inputRequest.DateFrom)
	if inputData.DateFrom != "" {
		inputData.DateTo = promptForInputData("dateTo", inputRequest.DateTo)
	}

	// Update below as new regions switch over to the new Eks
	if inputData.Env == "dev" ||
		(inputData.Env == "prod" && inputData.Region == "na1") ||
		(inputData.Env == "prod" && inputData.Region == "na2") ||
		(inputData.Env == "prod" && inputData.Region == "au1") ||
		(inputData.Env == "prod" && inputData.Region == "jp1") ||
		(inputData.Env == "prod" && inputData.Region == "eu1") ||
		(inputData.Env == "prod" && inputData.Region == "uk1") ||
		(inputData.Env == "prod" && inputData.Region == "ca1") ||
		(inputData.Env == "prod" && inputData.Region == "ae1") {
		apiObjects.DmwApi.UrlPrefix = "https://digi-"
		apiObjects.DmwApi.Port = "443"
		apiObjects.DmwGrpc.UriPrefix = "digi-"
		apiObjects.DmwGrpc.Port = "443"
	}
	if inputData.Env == "localhost" {
		dmwApi.UrlPrefix = "http://localhost:"
		dmwGrpc.UriPrefix = "localhost:"
	}

	// Build api and gRPC URIs
	cxoneGetServiceTokenApiUrl, logFile := buildUri("cxoneGetToken", inputData, apiObjects, logFile)
	cxoneGetTenantByIdApiUrl, logFile := buildUri("cxoneGetTenant", inputData, apiObjects, logFile)
	dmwContactStateApiUrl, logFile := buildUri("dmwGetStates", inputData, apiObjects, logFile)
	dfoAuthTokenApiUrl, logFile := buildUri("dfoAuthToken", inputData, apiObjects, logFile)
	dfoGetChannelsApiUrl, logFile := buildUri("dfoGetContacts", inputData, apiObjects, logFile)
	dfoContactSearchApiUrl, logFile := buildUri("dfoContactSearch", inputData, apiObjects, logFile)
	dfoContactByIdApiUrl, logFile := buildUri("dfoContactById", inputData, apiObjects, logFile)
	dmwGrpcApiUrl, logFile := buildUri("dmwGrpc", inputData, apiObjects, logFile)

	// Get DFO auth Token and CxOne Service Token asynchronously
	wg.Add(2)

	//getDfoAuthToken
	var dfoAuthTokenObj models.DfoAuthTokenObj
	go func() {
		defer wg.Done()
		var err error
		t := time.Now()

		logMsg = fmt.Sprintf("begin api call: %s\n", getDfoAuthTokenOp)
		logFile = createLog(logMsg, logFile)

		dfoAuthTokenObj, err = getDfoAuthToken(dfoAuthTokenApiUrl, inputData)
		if err != nil {
			logMsg = fmt.Sprintf(err.Error())
			logFile = createLog(logMsg, logFile)
			return
		}
		logMsg = fmt.Sprintf("[%s] dfo auth Token successfully retrieved, duration=%s\n", getDfoAuthTokenOp, time.Since(t))
		logFile = createLog(logMsg, logFile)
	}()

	//getServiceToken
	var cxOneToken models.CxoneAuthTokenObj
	go func() {
		defer wg.Done()
		var err error
		t := time.Now()

		logMsg = fmt.Sprintf("begin api call: %s\n", getServiceTokenOp)
		logFile = createLog(logMsg, logFile)

		cxOneToken, err = getServiceToken(cxoneGetServiceTokenApiUrl, inputData)
		if err != nil {
			cxOneToken.Error = err
			logMsg = fmt.Sprintf(err.Error())
			logFile = createLog(logMsg, logFile)
			return
		}
		logMsg = fmt.Sprintf("[%s] cxone service token successfully retrieved, duration=%s\n", getServiceTokenOp, time.Since(t))
		logFile = createLog(logMsg, logFile)
	}()
	wg.Wait()

	// Get CxOne Tenant data and DFO Channel data asynchronously
	//getTenantData
	var tenantData models.TenantWrapper
	if cxOneToken.Error == nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			t := time.Now()

			logMsg = fmt.Sprintf("begin api call: %s\n", getTenantDataOp)
			logFile = createLog(logMsg, logFile)

			if cxOneToken.Error == nil {
				tenantData, err = getTenantData(cxoneGetTenantByIdApiUrl, inputData, cxOneToken)
				if err != nil {
					tenantData.Error = err
					logMsg = fmt.Sprintf(err.Error())
					logFile = createLog(logMsg, logFile)
					return
				}
				logMsg = fmt.Sprintf("[%s] successfully returned tenant data for tenantId: [%s], duration=%s\n", getTenantDataOp, inputData.TenantId, time.Since(t))
				logFile = createLog(logMsg, logFile)
			}
		}()
	} else {
		logMsg = fmt.Sprintf("unable to get service token to make cxone api calls to %s - terminating service", getTenantDataOp)
		logFile = createLog(logMsg, logFile)
		return
	}

	//getChannels
	var channels []models.ChannelData
	if dfoAuthTokenObj.Error == nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			t := time.Now()

			logMsg = fmt.Sprintf("begin api call: %s\n", getChannelsOp)
			logFile = createLog(logMsg, logFile)

			channels, err = getChannels(dfoGetChannelsApiUrl, dfoAuthTokenObj)
			if err != nil {
				logMsg = fmt.Sprintf(err.Error())
				logFile = createLog(logMsg, logFile)
				return
			}
			logMsg = fmt.Sprintf("[%s] returned [%v] total channels, duration=%s\n", getChannelsOp, len(channels), time.Since(t))
			logFile = createLog(logMsg, logFile)
		}()
	} else {
		logMsg = fmt.Sprintf("unable to get dfo auth token to make dfo api calls to %s - terminating service", getChannelsOp)
		logFile = createLog(logMsg, logFile)
		return
	}
	wg.Wait()
	// Get list of Digimiddleware known active contacts and DFO active contacts asynchronously
	//makeDfoContactSearchApiCall
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		t := time.Now()

		logMsg = fmt.Sprintf("begin api call: [%s]\n", makeDfoContactSearchApiCallOp)
		logFile = createLog(logMsg, logFile)

		dfoDataList, logFile, err = makeDfoContactSearchApiCall(dfoContactSearchApiUrl, inputData, dfoAuthTokenObj, logFile)
		if err != nil {
			dfoData.Err = err

			logMsg = fmt.Sprintf("error calling [%s]]: [%v]\n", makeDfoContactSearchApiCallOp, err)
			logFile = createLog(logMsg, logFile)

			return
		}
		logMsg = fmt.Sprintf("[%s] - done, duration=%s, dfoActiveContacts=%d\n", makeDfoContactSearchApiCallOp, time.Since(t), len(dfoDataList))
		logFile = createLog(logMsg, logFile)
	}()

	//getDmwActiveContactStateData
	if dfoAuthTokenObj.Error == nil { //Put if statement above both DMW and DFO calls as if we are unable to get a list of contacts from DFO, there is no sense calling DMW either.
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			t := time.Now()
			tenants[0] = inputData.TenantId

			logMsg = fmt.Sprintf("begin api call: [%s]\n", getDmwActiveContactStateDataOp)
			logFile = createLog(logMsg, logFile)

			dmwKnownContact, err = getDmwActiveContactStateData(dmwContactStateApiUrl, tenants)
			if err != nil {
				dmwKnownContact.Error = err
				logMsg = fmt.Sprintf(err.Error())
				logFile = createLog(logMsg, logFile)
				return
			}
			logMsg = fmt.Sprintf("[%s] successfully returned [%d] total contacts, duration=%s\n", getDmwActiveContactStateDataOp, len(dmwKnownContact.Contacts), time.Since(t))
			logFile = createLog(logMsg, logFile)
		}()
	} else {
		logMsg = fmt.Sprintf("unable to get dfo auth token to make dfo api calls to %s - terminating service", makeDfoContactSearchApiCallOp)
		logFile = createLog(logMsg, logFile)
		return
	}
	wg.Wait()

	// Compare lists to get the contacts that exist in DMW but are closed in DFO.
	var deltaContacts models.DmwKnownContacts

	if dmwKnownContact.Error == nil && dfoData.Err == nil {
		if len(dmwKnownContact.Contacts) > 0 {
			logMsg = fmt.Sprintf("begin building list: [%s]\n", buildDeltaListOp)
			logFile = createLog(logMsg, logFile)

			// Create a Map of the DFO Data list
			dfoDataMap := make(map[int64]models.DataView)
			for _, dfoc := range dfoDataList {
				dfoContactId, _ := strconv.ParseInt(dfoc.Id, 10, 64)
				dfoDataMap[dfoContactId] = dfoc
			}

			t := time.Now()
			//deltaContacts, logFile = buildDeltaList(inputData, dmwKnownContact, dfoDataList, logFile)
			deltaContacts, logFile = buildDeltaListMap(inputData, dmwKnownContact, dfoDataMap, logFile)

			logMsg = fmt.Sprintf("[%s] - done, duration=%s, deltaContacts=%d\n", buildDeltaListOp, time.Since(t), len(deltaContacts.Contacts))
			logFile = createLog(logMsg, logFile)

		} else {
			logMsg = fmt.Sprintf("dmw list was empty due to no contacts or error retrieving data from api - will not attempt to process updates")
			logFile = createLog(logMsg, logFile)

			return
		}
	} else {
		if dmwKnownContact.Error != nil {
			logMsg = fmt.Sprintf("error retrieving data from [getDmwActiveContactStateData] - will not attempt to process updates")
		}
		if dfoData.Err != nil {
			logMsg = fmt.Sprintf("error retrieving data from [makeDfoContactSearchApiCall] - will not attempt to process updates")
		}
		logFile = createLog(logMsg, logFile)

		return
	}

	// Batch and Process records to digimiddleware using gRPC
	if len(deltaContacts.Contacts) > 0 {
		logFile = process(deltaContacts.Contacts, dfoAuthTokenObj, dfoContactByIdApiUrl, dmwGrpcApiUrl, tenantData.Tenant, channels, logFile)
	} else {
		logMsg = fmt.Sprintf("comparison of lists returned 0 contacts to update - will not attempt to process updates")
		logFile = createLog(logMsg, logFile)

		return
	}

	logMsg = fmt.Sprintf("update case state service completed - totalDuration = %s\n", time.Since(st))
	logFile = createLog(logMsg, logFile)

	// Create output logFile file
	filepath := fmt.Sprintf("D:\\ContactCloseUpdates\\Logs\\LOG_%s_%s_%d", tenantData.Tenant.ClusterId, tenantData.Tenant.BillingId, time.Now().UnixNano())

	writeLogFile(".csv", filepath, logFile)
}

// process batches the data into specified batchSize to process
func process(deltaContacts []models.DmwKnownContact, dfoAuthTokenObj models.DfoAuthTokenObj, dfoContactByIdApiUrl, dmwGrpcApiUrl string, tenant models.Tenant, channels []models.ChannelData, log []byte) []byte {
	batchCount := 1
	var errList []models.DataView
	var logMsg string
	var mtx sync.Mutex
	var sanitizedUpdateRecords []*pbm.CaseUpdateEvent
	var updateMessage *pbm.CaseUpdateEvent
	var updatedRecords []*pbm.CaseUpdateEvent

	for start, end := 0, 0; start <= len(deltaContacts)-1; start = end {
		var err error

		end = start + batchSize
		if end > len(deltaContacts) {
			end = len(deltaContacts)
		}
		batch := deltaContacts[start:end]

		t := time.Now()

		logMsg = fmt.Sprintf("begin processing batch [%d]\n", batchCount)
		log = createLog(logMsg, log)

		sanitizedUpdateRecords, log, errList = processBatch(batch, dfoContactByIdApiUrl, dfoAuthTokenObj, tenant, channels, log)

		logMsg = fmt.Sprintf("[%s] [%d] - done, duration=%s, total records to update=%d\n", processBatchOp, batchCount, time.Since(t), len(sanitizedUpdateRecords))
		log = createLog(logMsg, log)

		if errList != nil {
			// Range over errList to print all errors together for more readable logs
			for _, e := range errList {
				logMsg = fmt.Sprintln(e.Err)
				log = createLog(logMsg, log)
			}

			//make a map for deltaContacts
			deltaContactsMap := make(map[int64]models.DmwKnownContact)
			for _, dc := range deltaContacts {
				deltaContactsMap[dc.ContactID] = dc
			}
			//make a map for channels
			channelsMap := make(map[string]models.ChannelData)
			for _, c := range channels {
				channelsMap[c.ChannelId] = c
			}

			for _, e := range errList {
				// For contacts not found in DFO, attempt to build a CaseUpdate message with data we are aware of
				updateMessage, err = processNotFoundEvent(deltaContactsMap, e, tenant, channelsMap)
				if err != nil {
					err = fmt.Errorf("[%s] unable to create case update event for contactId: [%s] - no further attempts to update contact will be made\n[ERROR]: %v\n", processNotFoundEventOp, e.Id, err)
					log = createLog(logMsg, log)
					break
				}

				if updateMessage != nil {
					mtx.Lock()
					sanitizedUpdateRecords = append(sanitizedUpdateRecords, updateMessage)
					mtx.Unlock()
				} else {
					logMsg = fmt.Sprintf("[%s] - unable to create update message for contactId [%s]\n", processNotFoundEventOp, e.Id)
					log = createLog(logMsg, log)
				}
			}
			//}
		}
		batchCount++

		// Push sanitizedUpdateRecords to digimiddleware via gRPC
		if sanitizedUpdateRecords != nil {
			logMsg = fmt.Sprintf("begin pushing updates to digimiddleware: [%s]\n", sendUpdateRecordsToMiddlewareOp)
			log = createLog(logMsg, log)
			logMsg = fmt.Sprintf("[%s] total count of case event updates to be sent to digimiddleware [%v]\n", sendUpdateRecordsToMiddlewareOp, len(sanitizedUpdateRecords))
			log = createLog(logMsg, log)

			t := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 300000*time.Millisecond)

			log, err = sendUpdateRecordsToMiddleware(ctx, &pbm.CaseUpdateEvents{
				Updates:    sanitizedUpdateRecords,
				ReceivedAt: timestamppb.Now(),
			}, dmwGrpcApiUrl, log)

			if err != nil {
				logMsg = fmt.Sprintln(err.Error())
				log = createLog(logMsg, log)
				cancel()
				return log
			}
			cancel()
			logMsg = fmt.Sprintf("[%s] - done, duration=%s, count=%d\n", sendUpdateRecordsToMiddlewareOp, time.Since(t), len(sanitizedUpdateRecords))
			log = createLog(logMsg, log)

			mtx.Lock()
			updatedRecords = append(updatedRecords, sanitizedUpdateRecords...)
			mtx.Unlock()
		} else {
			logMsg = fmt.Sprintf("[%s] there were no contacts added to sanitizedUpdateRecords list - will not attempt to process updates\n", processBatchOp)
			log = createLog(logMsg, log)
		}
		// Sleep between processing batches to not overload Digipro pushes to the VC
		time.Sleep(20 * time.Second)
	}

	// Range over updatedRecords to print all update record objects that were sent to dmw for update
	for _, record := range updatedRecords {
		logMsg = fmt.Sprintln(record)
		log = createLog(logMsg, log)
	}

	// Create output file and print updated contacts to console
	filepath := fmt.Sprintf("D:\\ContactCloseUpdates\\%s_%s_%d", tenant.ClusterId, tenant.BillingId, time.Now().UnixNano())

	mr, _ := json.Marshal(updatedRecords)
	writeLogFile(".csv", filepath, mr)

	return log
}

// processBatch calls DFO 3.0 GET Contact to obtain contact data to build the case update event
func processBatch(list []models.DmwKnownContact, dfoContactByIdApiUrl string, dfoAuthTokenObj models.DfoAuthTokenObj, tenant models.Tenant, channels []models.ChannelData, log []byte) ([]*pbm.CaseUpdateEvent, []byte, []models.DataView) {
	var errList []models.DataView
	var sanitizedUpdateRecords []*pbm.CaseUpdateEvent
	var wg sync.WaitGroup

	for _, contact := range list {
		wg.Add(1)
		go func(contact models.DmwKnownContact) {
			defer wg.Done()
			var contactData models.DataView
			var l []byte
			var mtx sync.Mutex

			// Call DFO api for each record in delta list to obtain actual metadata for contact
			contactData, l = getDfoContactData(dfoContactByIdApiUrl, dfoAuthTokenObj, contact, l)

			if contactData.Err == nil {
				// Transform contact data into an updateMessage event to send to dmw gRPC
				updateMessage := buildShipment(contactData, errList, tenant, channels)

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
		}(contact)
	}
	wg.Wait()

	return sanitizedUpdateRecords, log, errList
}

// buildShipment transforms the contact data into a case update event message
func buildShipment(contactData models.DataView, errList []models.DataView, tenant models.Tenant, channels []models.ChannelData) *pbm.CaseUpdateEvent {
	//var mtx sync.Mutex

	// Create the Update Event object from the contactData received from DFO
	event := createEvent(contactData, tenant, channels)

	// Using the Event Object, create the CaseUpdateEvent object
	updateMessage, err := makeCaseUpdate(event)
	if err != nil {
		contactData.Err = err
		//mtx.Lock()
		//errList = append(errList, contactData)
		//mtx.Unlock()
		return nil
	}

	return updateMessage
}

// processNotFoundEvent will build an event using fake data and push event to DMW to clean up the stuck contacts that were created with automation (not found in DFO)
func processNotFoundEvent(deltaContactsMap map[int64]models.DmwKnownContact, notFoundContact models.DataView, tenant models.Tenant, channelsMap map[string]models.ChannelData) (*pbm.CaseUpdateEvent, error) {
	b, _ := strconv.ParseInt(tenant.BillingId, 10, 32)
	businessUnitNo := int32(b)
	var channelIdOnExternalPlatform string
	var channelIntegrationBoxIdentifier string
	var channelIsPrivate bool
	var channelName string
	var channelRealExternalPlatformId string
	var direction string
	var event models.StreamEventRequest
	var err error
	randomGuid := uuid.New().String()
	var recipients []models.Recipient
	var updateMessage *pbm.CaseUpdateEvent

	contactId, _ := strconv.ParseInt(notFoundContact.Id, 10, 64)

	//check map for key to contactID
	dc, ok := deltaContactsMap[contactId]
	if ok {
		// Convert the int to correct string direction
		if dc.Direction == 0 {
			direction = "outbound"
		} else {
			direction = "inbound"
		}

		// Range through channel list and populate the channel data
		//check channelsMap for key
		channel, ok := channelsMap[dc.ChannelID]
		if ok {
			channelIdOnExternalPlatform = channel.IdOnExternalPlatform
			channelIntegrationBoxIdentifier = channel.IntegrationBoxIdent
			channelIsPrivate = channel.IsPrivate
			channelName = channel.Name
			channelRealExternalPlatformId = channel.RealExternalPlatformId
		}

		// We are not able to obtain the exact contact data since these contacts do not exist in DFO so update with the data we are aware of
		event.Data.Contact.ID = notFoundContact.Id
		event.CreatedAt = dc.CurrentContactDate
		event.EventID = uuid.New().String()
		event.EventObject = models.EventObject_Case
		event.EventType = 1        // TYPE_STATUS_CHANGED
		event.Data.Brand.ID = 9999 // we don't use this so hardCode anything
		event.Data.Brand.TenantID = tenant.TenantId
		event.Data.Brand.BusinessUnitID = businessUnitNo
		event.Data.Case.ID = dc.CaseIDString
		event.Data.Case.ContactId = uuid.New().String()         // unknown
		event.Data.Case.CustomerContactId = uuid.New().String() // unknown
		event.Data.Case.DetailUrl = ""                          // unknown
		event.Data.Case.EndUserRecipients = recipients          // unknown
		event.Data.Case.InboxAssignee = 0                       // unknown
		event.Data.Case.InteractionId = uuid.New().String()     // unknown
		event.Data.Case.Direction = direction
		event.Data.Case.ThreadId = uuid.New().String() // unknown
		event.Data.Case.RoutingQueueId = dc.QueueID
		event.Data.Case.RoutingQueuePriority = 1 // unknown
		event.Data.Case.Status = "closed"        // hard-code to endContact
		event.Data.Case.OwnerAssignee = 0        // unknown
		event.Data.Channel.ID = dc.ChannelID
		event.Data.Channel.Name = channelName
		event.Data.Channel.IntegrationBoxIdentifier = channelIntegrationBoxIdentifier
		event.Data.Channel.IDOnExternalPlatform = channelIdOnExternalPlatform
		event.Data.Channel.IsPrivate = channelIsPrivate
		event.Data.Channel.RealExternalPlatformID = channelRealExternalPlatformId
		event.Data.Case.AuthorUser.ID = 0
		event.Data.Case.AuthorUser.InContactID = ""
		event.Data.Case.AuthorUser.EmailAddress = ""
		event.Data.Case.AuthorUser.LoginUsername = ""
		event.Data.Case.AuthorUser.FirstName = ""
		event.Data.Case.AuthorUser.SurName = ""
		event.Data.Case.AuthorEndUserIdentity.ID = "automation_" + randomGuid
		event.Data.Case.AuthorEndUserIdentity.IdOnExternalPlatform = randomGuid
		event.Data.Case.AuthorEndUserIdentity.FullName = "automation test "

		updateMessage, err = makeCaseUpdate(event)
		if err != nil {
			return nil, err
		}
	}

	return updateMessage, nil
}

// buildUri builds the uri for each api or grpc call needed
func buildUri(apiType string, i InputDataObj, apiObjects ApiObjects, log []byte) (string, []byte) {
	uri := ""
	logMsg := fmt.Sprintf("%s uri for requested region [%s] and env [%s] -- ", apiType, i.Region, i.Env)
	log = createLog(logMsg, log)

	switch apiType {
	case "cxoneGetTenant":
		switch i.Env {
		case "prod":
			switch i.Region {
			case "na1", "au1":
				uri = apiObjects.CxOneApi.UrlPrefix + i.Region + apiObjects.CxOneApi.Path + apiObjects.CxOneApi.GetTenantByIdEndpoint
				logMsg = fmt.Sprintln(uri)
				log = createLog(logMsg, log)
			case "eu1", "jp1", "uk1", "ca1":
				uri = apiObjects.CxOneApi.UrlPrefix + i.Region + apiObjects.CxOneApi.PathNoDash + apiObjects.CxOneApi.GetTenantByIdEndpoint
				logMsg = fmt.Sprintln(uri)
				log = createLog(logMsg, log)
			default:
				break
			}
		case "dev", "test", "staging":
			uri = apiObjects.CxOneApi.UrlPrefix + i.Region + "." + i.Env + apiObjects.CxOneApi.Path + apiObjects.CxOneApi.GetTenantByIdEndpoint
			logMsg = fmt.Sprintln(uri)
			log = createLog(logMsg, log)
		default:
			break
		}
	case "cxoneGetToken":
		switch i.Env {
		case "prod":
			switch i.Region {
			case "na1", "au1":
				uri = apiObjects.CxOneApi.UrlPrefix + i.Region + apiObjects.CxOneApi.Path + apiObjects.CxOneApi.GetTokenEndpoint
				logMsg = fmt.Sprintln(uri)
				log = createLog(logMsg, log)
			case "eu1", "jp1", "uk1", "ca1":
				uri = apiObjects.CxOneApi.UrlPrefix + i.Region + apiObjects.CxOneApi.PathNoDash + apiObjects.CxOneApi.GetTokenEndpoint
				logMsg = fmt.Sprintln(uri)
				log = createLog(logMsg, log)
			default:
				break
			}
		case "dev", "test", "staging":
			uri = apiObjects.CxOneApi.UrlPrefix + i.Region + "." + i.Env + apiObjects.CxOneApi.Path + apiObjects.CxOneApi.GetTokenEndpoint
			logMsg = fmt.Sprintln(uri)
			log = createLog(logMsg, log)
		default:
			break
		}
	case "dmwGetStates":
		uri = apiObjects.DmwApi.UrlPrefix + i.Region + ".omnichannel." + i.Env + ".internal:" + apiObjects.DmwApi.Port + apiObjects.DmwApi.Path + apiObjects.DmwApi.GetStatesEndpoint
		if i.Env == "localhost" {
			uri = apiObjects.DmwApi.UrlPrefix + apiObjects.DmwApi.Port + apiObjects.DmwApi.Path + apiObjects.DmwApi.GetStatesEndpoint
		}

		logMsg = fmt.Sprintln(uri)
		log = createLog(logMsg, log)
	case "dfoAuthToken":
		switch i.Env {
		case "prod":
			uri = apiObjects.DfoApi.UrlPrefix + i.Region + apiObjects.DfoApi.V2Path + apiObjects.DfoApi.AuthTokenEndpoint
			logMsg = fmt.Sprintln(uri)
			log = createLog(logMsg, log)
		case "dev", "test", "staging":
			uri = apiObjects.DfoApi.UrlPrefix + i.Region + "." + i.Env + apiObjects.DfoApi.V2Path + apiObjects.DfoApi.AuthTokenEndpoint
			logMsg = fmt.Sprintln(uri)
			log = createLog(logMsg, log)
		default:
			break
		}
	case "dfoGetContacts":
		switch i.Env {
		case "prod":
			uri = apiObjects.DfoApi.UrlPrefix + i.Region + apiObjects.DfoApi.V3Path + apiObjects.DfoApi.GetChannelsEndpoint
			logMsg = fmt.Sprintln(uri)
			log = createLog(logMsg, log)
		case "dev", "test", "staging":
			uri = apiObjects.DfoApi.UrlPrefix + i.Region + "." + i.Env + apiObjects.DfoApi.V3Path + apiObjects.DfoApi.GetChannelsEndpoint
			logMsg = fmt.Sprintln(uri)
			log = createLog(logMsg, log)
		default:
			break
		}
	case "dfoContactSearch": // only requesting contacts in an active status to decrease load on api - assume if contact does not exist in response, it is closed or trashed
		switch i.Env {
		case "prod":
			uri = apiObjects.DfoApi.UrlPrefix + i.Region + apiObjects.DfoApi.V3Path + apiObjects.DfoApi.GetContactSearchEndpoint + apiObjects.DfoApi.ContactSearchActiveContactsFilter
			logMsg = fmt.Sprintln(uri)
			log = createLog(logMsg, log)
		case "dev", "test", "staging":
			uri = apiObjects.DfoApi.UrlPrefix + i.Region + "." + i.Env + apiObjects.DfoApi.V3Path + apiObjects.DfoApi.GetContactSearchEndpoint + apiObjects.DfoApi.ContactSearchActiveContactsFilter
			logMsg = fmt.Sprintln(uri)
			log = createLog(logMsg, log)
		default:
			break
		}
	case "dfoContactById":
		switch i.Env {
		case "prod":
			uri = apiObjects.DfoApi.UrlPrefix + i.Region + apiObjects.DfoApi.V3Path + apiObjects.DfoApi.GetContactByIdEndpoint
			logMsg = fmt.Sprintln(uri)
			log = createLog(logMsg, log)
		case "dev", "test", "staging":
			uri = apiObjects.DfoApi.UrlPrefix + i.Region + "." + i.Env + apiObjects.DfoApi.V3Path + apiObjects.DfoApi.GetContactByIdEndpoint
			logMsg = fmt.Sprintln(uri)
			log = createLog(logMsg, log)
		default:
			break
		}
	case "dmwGrpc":
		uri = apiObjects.DmwGrpc.UriPrefix + i.Region + ".omnichannel." + i.Env + ".internal:" + apiObjects.DmwGrpc.Port
		if i.Env == "localhost" {
			uri = apiObjects.DmwGrpc.UriPrefix + apiObjects.DmwGrpc.Port
		}
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

	if response == "localhost" {
		fmt.Printf("NOTE: must do port-forward if running on localhost: kubectl port-forward svc/voyager-digi-ingress 8085 9884 -n digi")
	}

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

	return strings.TrimSuffix(response, "\r\n")
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
	case "accessKeyCreds":
		if len(inputValue) != 56 {
			fmt.Println("INPUT VALUE IS NOT THE PROPER STRING LENGTH (56)")
			return false
		} else {
			return true
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
	default:
		return false
	}
}

// getServiceToken calls the CxOne API POST token/access-key using the T0 user accessKeyId and accessKeySecret and returns a service token for the env
func getServiceToken(apiUrl string, i InputDataObj) (models.CxoneAuthTokenObj, error) {
	var cxoneServiceTokenBody models.CxoneAuthTokenBody
	cxoneServiceTokenBody.AccessKeyId = i.AccessKeyId
	cxoneServiceTokenBody.AccessKeySecret = i.AccessKeySecret
	var tokenObj models.CxoneAuthTokenObj
	var responseData []byte

	bodyJson, _ := json.Marshal(cxoneServiceTokenBody)
	reader := bytes.NewReader(bodyJson)

	response, err := http.Post(apiUrl, contentType, reader)
	if err != nil {
		err = fmt.Errorf("[%s] returned error getting response - check uri or accessKeyId and accessKeySecret - error: %v\n", getServiceTokenOp, err)
		return tokenObj, err
	}

	if response != nil {
		if response.StatusCode == 200 {
			responseData, err = io.ReadAll(response.Body)
			if err != nil {
				err = fmt.Errorf("[%s] returned error reading response body - error: %v\n", getServiceTokenOp, err)
				return tokenObj, err
			}
		} else {
			err = fmt.Errorf(getServiceTokenOp+httpBadResponse, response.StatusCode, response.Status)
			return tokenObj, err
		}
	}

	if responseData != nil {
		err = json.Unmarshal(responseData, &tokenObj)
		if err != nil {
			err = fmt.Errorf("[%s] cannot unmarshal cxone service token: [%v]\n", getServiceTokenOp, err)
			return tokenObj, err
		}
	} else {
		err = fmt.Errorf("[%s] cxone service token response was null or empty\n", getServiceTokenOp)
		return tokenObj, err
	}

	return tokenObj, nil
}

// getDfoAuthToken calls the DFO api POST engager/2.0/token and returns the auth token for subsequent DFO api calls
func getDfoAuthToken(apiUrl string, i InputDataObj) (models.DfoAuthTokenObj, error) {
	var dfoAuthTokenBody models.DfoAuthTokenBody
	dfoAuthTokenBody.GrantType = "client_credentials"
	dfoAuthTokenBody.ClientId = i.ClientId
	dfoAuthTokenBody.ClientSecret = i.ClientSecret
	var dfoAuthTokenObj models.DfoAuthTokenObj
	var responseData []byte

	bodyJson, _ := json.Marshal(dfoAuthTokenBody)
	reader := bytes.NewReader(bodyJson)

	response, err := http.Post(apiUrl, contentType, reader)
	if err != nil {
		err = fmt.Errorf("[%s] returned error getting response - check uri or clientId and clientSecret - error: %v\n", getDfoAuthTokenOp, err)
		return dfoAuthTokenObj, err
	}

	if response.StatusCode == 200 {
		responseData, err = io.ReadAll(response.Body)
		if err != nil {
			err = fmt.Errorf("[%s] returned error reading response body - error: %v\n", getDfoAuthTokenOp, err)
			return dfoAuthTokenObj, err
		}
	} else {
		err = fmt.Errorf(getDfoAuthTokenOp+httpBadResponse, response.StatusCode, response.Status)
		return dfoAuthTokenObj, err
	}

	if responseData != nil {
		err = json.Unmarshal(responseData, &dfoAuthTokenObj)
		if err != nil {
			err = fmt.Errorf("[%s] cannot unmarshal dfo auth token response: [%v]\n", getDfoAuthTokenOp, err)
			return dfoAuthTokenObj, err
		}
	} else {
		err = fmt.Errorf("[%s] dfo auth token response was null or empty\n", getDfoAuthTokenOp)
		return dfoAuthTokenObj, err
	}

	return dfoAuthTokenObj, nil
}

// getTenantData calls the CxOne api GET tenant by tenantId and returns the Tenant object
func getTenantData(cxoneGetTenantByIdApiUrl string, i InputDataObj, token models.CxoneAuthTokenObj) (models.TenantWrapper, error) {
	var cxoneResponse []byte
	var err error
	var tenantObj models.TenantWrapper

	if i.TenantId != "" {
		cxoneGetTenantByIdApiUrl = cxoneGetTenantByIdApiUrl + i.TenantId
	} else {
		err = fmt.Errorf("[%s] unable to request tenant data - no tenantId was supplied\n", getTenantDataOp)
		return tenantObj, err
	}

	cxoneResponse, err = makeCxOneApiCall(cxoneGetTenantByIdApiUrl, token, i.TenantId, "GET")
	if err != nil {
		return tenantObj, err
	}
	if cxoneResponse != nil {
		err = json.Unmarshal(cxoneResponse, &tenantObj)
		if err != nil {
			err = fmt.Errorf("[%s] cannot unmarshal cxone tenant data response - error: [%s]\n", getTenantDataOp, err)
			return tenantObj, err
		}
	}

	return tenantObj, nil
}

// getChannels calls DFO v3.0 GET Channels and returns an array of all channel data
func getChannels(dfoGetChannelsApiUrl string, dfoAuthTokenObj models.DfoAuthTokenObj) ([]models.ChannelData, error) {
	var dfoChannelsList []models.ChannelData

	// Call DFO 3.0 GET Channels
	dfoResponse, err := makeDfoApiCall(dfoGetChannelsApiUrl, dfoAuthTokenObj, "GET", "")
	if err != nil {
		return nil, err
	}
	if dfoResponse != nil {
		err = json.Unmarshal(dfoResponse, &dfoChannelsList)
		if err != nil {
			err = fmt.Errorf("[%s] cannot unmarshal dfo channel response: [%v]\n", getChannelsOp, err)
			return nil, err
		}
	} else {
		err = fmt.Errorf("[%s] dfo channels response was null or empty\n", getChannelsOp)
		return nil, err
	}

	sort.Sort(models.ChannelSort(dfoChannelsList))
	return dfoChannelsList, nil
}

// getDmwActiveContactStateData calls the Digimiddleware api POST digimiddleware/getstatesbytenants to get the list of contacts stored in DynamoDB
func getDmwActiveContactStateData(apiUrl string, tenants [1]string) (models.DmwKnownContacts, error) {
	var dmwKnownContact models.DmwKnownContacts
	var tenantIdsObj TenantIdsWrapper
	tenantIdsObj.TenantIDs = tenants
	var responseData []byte

	bodyJson, _ := json.Marshal(tenantIdsObj)
	reader := bytes.NewReader(bodyJson)

	response, err := http.Post(apiUrl, contentType, reader)
	if err != nil {
		err = fmt.Errorf("[%s] returned error getting response - check uri or tenantId - error: %v\n", getDmwActiveContactStateDataOp, err)
		return dmwKnownContact, err
	}

	if response.StatusCode == 200 {
		responseData, err = io.ReadAll(response.Body)
		if err != nil {
			err = fmt.Errorf("[%s] returned error reading response body - error: %v\n", getDmwActiveContactStateDataOp, err)
			return dmwKnownContact, err
		}
	} else {
		err = fmt.Errorf(getDmwActiveContactStateDataOp+httpBadResponse, response.StatusCode, response.Status)
		return dmwKnownContact, err
	}

	if responseData != nil {
		err = json.Unmarshal(responseData, &dmwKnownContact)
		if err != nil {
			err = fmt.Errorf("[%s] cannot unmarshal dmw contact state response: [%v]\n", getDmwActiveContactStateDataOp, err)
			return dmwKnownContact, err
		}
	}

	sort.Sort(models.DmwKnownContactSort(dmwKnownContact.Contacts))
	return dmwKnownContact, err
}

// makeDfoContactSearchApiCall calls the DFO 3.0 GET Contact Search Api then loops until all data is retrieved (api only returns 25 records at a time)
func makeDfoContactSearchApiCall(dfoContactSearchApiUrl string, i InputDataObj, dfoAuthTokenObj models.DfoAuthTokenObj, log []byte) ([]models.DataView, []byte, error) {
	var apiUrl DfoApiUrlObj
	var dfoActiveContactList models.DfoContactSearchResponse
	var dfoData []models.DataView
	var hits int32 = 25
	var logMsg string
	var mtx sync.Mutex

	// Add URL parameters
	apiUrl.DateFrom = "&date[from]=" + i.DateFrom + "&"
	apiUrl.DateTo = "&date[to]=" + i.DateTo + "&"
	apiUrl.ScrollToken = "&scrollToken="
	apiUrl.Sorting = "&sorting=createdAt"
	apiUrl.SortingType = "&sortingType=asc"
	if i.DateFrom == "" {
		apiUrl.Url = dfoContactSearchApiUrl
	} else if i.DateTo == "" {
		apiUrl.Url = dfoContactSearchApiUrl + apiUrl.DateFrom + apiUrl.Sorting + apiUrl.SortingType
	} else {
		apiUrl.Url = dfoContactSearchApiUrl + apiUrl.DateFrom + apiUrl.DateTo + apiUrl.Sorting + apiUrl.SortingType
	}

	// Call DFO 3.0 GET Contact Search which returns 1st 25 records
	dfoResponse, err := makeDfoApiCall(apiUrl.Url, dfoAuthTokenObj, "GET", "")
	if err != nil {
		return nil, log, err
	}
	if dfoResponse != nil {
		err = json.Unmarshal(dfoResponse, &dfoActiveContactList)
		if err != nil {
			err = fmt.Errorf("[%s] cannot unmarshal dfo contact search response: [%v]\n", makeDfoContactSearchApiCallOp, err)
			return nil, log, err
		}
	}

	if dfoActiveContactList.Hits > batchSize {
		numberOfApiCalls := dfoActiveContactList.Hits / 25
		fmt.Printf("TOTAL ACTIVE DFO CONTACTS [%v] WHICH WILL MAKE [%d] API CALLS TO DFO. Type Y to continue or N to cancel: ", dfoActiveContactList.Hits, numberOfApiCalls)
		reader := bufio.NewReader(os.Stdin)

		response, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println(err)
		}

		response = strings.TrimSuffix(response, "\r\n")

		if response != "Y" {
			os.Exit(1) // Terminate the program
		}
	}

	if dfoActiveContactList.Hits == 0 {
		logMsg = fmt.Sprintf("[%s] returned 0 records\n", makeDfoContactSearchApiCallOp)
		log = createLog(logMsg, log)

		return nil, log, err
	}

	logMsg = fmt.Sprintf("[%s] returned [%v] total hits. (DFO limits calls to 25 records at a time.)\n", makeDfoContactSearchApiCallOp, dfoActiveContactList.Hits)
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

			logMsg = fmt.Sprintf("calling [%s] to get next set of records up to [%v]\n", makeDfoContactSearchApiCallOp, hits)
			log = createLog(logMsg, log)

			apiUrl.Url = dfoContactSearchApiUrl + apiUrl.ScrollToken + dfoActiveContactList.ScrollToken
			dfoResponse, err = makeDfoApiCall(apiUrl.Url, dfoAuthTokenObj, "GET", "")
			if err != nil {
				return nil, log, err
			}

			if dfoResponse != nil {
				err = json.Unmarshal(dfoResponse, &dfoActiveContactList)
				if err != nil {
					err = fmt.Errorf("[%s] cannot unmarshal dfo contact search response: [%v]\n", makeDfoContactSearchApiCallOp, err)
					return nil, log, err
				}

				// Append next set of Data records to Data list
				mtx.Lock()
				dfoData = append(dfoData, dfoActiveContactList.Data...)
				mtx.Unlock()
			}
		}
	}

	sort.Sort(models.DfoDataSort(dfoData))
	return dfoData, log, nil
}

// buildDeltaListMap loops through the known contacts from DMW and compares them to the active contacts in DFO and creates a list of contacts that need to be updated
func buildDeltaListMap(inputData InputDataObj, dmwKnownContact models.DmwKnownContacts, dfoDataMap map[int64]models.DataView, log []byte) (models.DmwKnownContacts, []byte) {
	type DmwContacts []models.DmwKnownContact
	deltaArray := DmwContacts{}
	var deltaList models.DmwKnownContacts
	foundCount := 0
	var logMsg string
	var mtx sync.Mutex
	notFoundCount := 0
	alreadyClosedCount := 0
	updateCount := 0

	// Loop through DmwKnownContact.Contacts and check each contact data in DfoActiveContacts to see if we find a match
	for _, contact := range dmwKnownContact.Contacts {
		convertedDate := time.Unix(contact.CurrentContactDate.Seconds, int64(contact.CurrentContactDate.Nanos)).UTC().Format(time.RFC3339)
		found := false
		shouldClose := false

		// Only compare contact with DFO data if contact is not closed (18)
		if contact.CurrentContactState != 18 {
			if len(dfoDataMap) > 0 {
				// Check dfoDataMap for corresponding ContactId
				d, ok := dfoDataMap[contact.ContactID]
				if ok {
					found = true
					foundCount++
					matchesDateFilter := true

					dfoDigitalContactState := digiservice.MapFSMStateToDigitalContactState(fsm.State(d.Status))
					// Check if current active states match - this print-out is strictly to be able to provide a list of contact ids that are in a miss-matched state
					// We will not attempt to update these contacts as active contacts are always in motion and due to delays in processing time, we do not want to introduce more miss-matched states
					if int32(dfoDigitalContactState) != contact.DigitalContactState {
						updateCount++

						logMsg = fmt.Sprintf("MissmatchedState:*ContactID*[%d]*Found*[%v]*DigimiddlewareDigitalContactState*[%d]*DfoDigitalContactState*[%d]*CurrentContactDate*[%v]\n", contact.ContactID, found, contact.DigitalContactState, int32(dfoDigitalContactState), convertedDate)
						log = createLog(logMsg, log)
					}

					// If date filter was added, only check dmw known contacts between those dates
					if inputData.DateFrom != "" {
						if convertedDate <= inputData.DateFrom || convertedDate >= inputData.DateTo {
							matchesDateFilter = false
						}

						if !matchesDateFilter {
							break
						}
					}
				} else {
					shouldClose = true
				}
			} else {
				shouldClose = true
			}

			if !found && shouldClose {
				notFoundCount++

				logMsg = fmt.Sprintf("ShouldClose:*ContactID*[%d]*Found*[%v]*DigimiddlewareDigitalContactState*[%d]*CurrentContactDate*[%v]\n", contact.ContactID, found, contact.DigitalContactState, convertedDate)
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
	logMsg = fmt.Sprintf("total contacts that were already closed (will NOT attempt to update): %d\n", alreadyClosedCount)
	log = createLog(logMsg, log)
	logMsg = fmt.Sprintf("total contacts that were not found (will attempt to update): %d\n", notFoundCount)
	log = createLog(logMsg, log)
	logMsg = fmt.Sprintf("total contacts that were found (will NOT attempt to update): %d\n", foundCount)
	log = createLog(logMsg, log)
	logMsg = fmt.Sprintf("total contacts that were found but had miss-matched state (will NOT attempt to update as active contacts are always in motion and we do not want to introduce more miss-matched states): %d\n", updateCount)
	log = createLog(logMsg, log)

	if notFoundCount > batchSize {
		timeToComplete := ((notFoundCount / batchSize) * 20) / 60
		fmt.Printf("TOTAL CONTACTS TO CLOSE [%d] WHICH WILL MAKE [%d] API CALLS TO DFO AND TAKE [%d] MINUTES TO COMPLETELY PROCESS. Type Y to continue or N to cancel: ", notFoundCount, notFoundCount, timeToComplete)
		reader := bufio.NewReader(os.Stdin)

		response, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println(err)
		}

		response = strings.TrimSuffix(response, "\r\n")

		if response != "Y" {
			os.Exit(1) // Terminate the program
		}
	}

	return deltaList, log
}

// getDfoContactData calls makeDfoApiCall to get each contact data by contact id
func getDfoContactData(dfoContactByIdApiUrl string, dfoAuthTokenObj models.DfoAuthTokenObj, contact models.DmwKnownContact, log []byte) (models.DataView, []byte) {
	var dfoClosedContactData models.DataView
	var logMsg string

	// Call DFO v3.0 GET contact by contactId
	dfoResponse, respErr := makeDfoApiCall(dfoContactByIdApiUrl, dfoAuthTokenObj, "GET", strconv.Itoa(int(contact.ContactID)))
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
		log = createLog(logMsg, log)
	}

	return dfoClosedContactData, log
}

// makeCxOneApiCall calls CxOne api GET tenant by tenantId and returns the tenant data object
func makeCxOneApiCall(apiUrl string, token models.CxoneAuthTokenObj, tenantId string, method string) ([]byte, error) {
	var bearer = token.TokenType + " " + token.IdToken
	client := &http.Client{}
	resp := &http.Response{}
	var responseData []byte

	// Create a new request using http
	req, err := http.NewRequest(method, apiUrl, nil)
	if err != nil {
		err = fmt.Errorf("[%s] attempt to create http.NewRequest returned an error: [%v]\n", makeCxOneApiCallOp, err)
		return responseData, err
	}
	if req != nil {
		// add authorization header and content-type to the req
		req.Header.Add("Authorization", bearer)
		req.Header.Add("Content-Type", contentType)
	}

	// Send req using http Client
	resp, err = client.Do(req)
	if err != nil {
		err = fmt.Errorf("[%s] error connecting to host\n[ERROR] - %v\n", makeCxOneApiCallOp, err)
		return responseData, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		responseData, err = io.ReadAll(resp.Body)
		if err != nil {
			err = fmt.Errorf("[%s] error reading response bytes: %v\n", makeCxOneApiCallOp, err)
			return responseData, err
		}
	} else {
		err = fmt.Errorf("[%s] error calling cxone api for tenant %s: %v", makeCxOneApiCallOp, tenantId, resp.Status)
		return responseData, err
	}

	return responseData, err
}

// makeDfoApiCall calls DFO 3.0 APIs and returns the response object
func makeDfoApiCall(apiUrl string, dfoAuthTokenObj models.DfoAuthTokenObj, method string, contactId string) ([]byte, error) {
	var bearer = dfoAuthTokenObj.TokenType + " " + dfoAuthTokenObj.AccessToken
	client := &http.Client{}
	resp := &http.Response{}
	var responseData []byte

	if contactId != "" {
		apiUrl = apiUrl + contactId
	}

	// Create a new request using http
	req, err := http.NewRequest(method, apiUrl, nil)
	if err != nil {
		err = fmt.Errorf("[%s] attempt to create http.NewRequest returned an error: [%v]\n", makeDfoApiCallOp, err)
		return responseData, err
	}
	if req != nil {
		// add authorization header and content-type to the req
		req.Header.Add("Authorization", bearer)
		req.Header.Add("Content-Type", contentType)
	}

	// Send req using http Client
	resp, err = client.Do(req)
	if err != nil {
		err = fmt.Errorf("[%s] error connecting to host\n[ERROR] - %v\n", makeDfoApiCallOp, err)
		return responseData, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		responseData, err = io.ReadAll(resp.Body)
		if err != nil {
			err = fmt.Errorf("[%s] error reading response bytes: %v\n", makeDfoApiCallOp, err)
			return responseData, err
		}
	} else {
		err = fmt.Errorf("[%s] error calling dfo api for contact %s: %v", makeDfoApiCallOp, contactId, resp.Status)
		return responseData, err
	}

	return responseData, err
}

func createEvent(contactData models.DataView, tenant models.Tenant, channels []models.ChannelData) models.StreamEventRequest {
	b, _ := strconv.ParseInt(tenant.BillingId, 10, 32)
	businessUnitNo := int32(b)
	var channelIdOnExternalPlatform string
	var channelIntegrationBoxIdentifier string
	var channelIsPrivate bool
	var channelName string
	var channelRealExternalPlatformId string
	var event models.StreamEventRequest

	for _, channel := range channels {
		if channel.ChannelId == contactData.ChannelId {
			channelIdOnExternalPlatform = channel.IdOnExternalPlatform
			channelIntegrationBoxIdentifier = channel.IntegrationBoxIdent
			channelIsPrivate = channel.IsPrivate
			channelName = channel.Name
			channelRealExternalPlatformId = channel.RealExternalPlatformId
		}
	}

	event.CreatedAt = contactData.StatusUpdatedAt
	event.EventID = uuid.New().String()
	event.EventObject = models.EventObject_Case
	event.EventType = 1        // TYPE_STATUS_CHANGED
	event.Data.Brand.ID = 9999 // we don't use this so hardCode anything
	event.Data.Brand.TenantID = tenant.TenantId
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
	event.Data.Channel.Name = channelName
	event.Data.Channel.IntegrationBoxIdentifier = channelIntegrationBoxIdentifier
	event.Data.Channel.IDOnExternalPlatform = channelIdOnExternalPlatform
	event.Data.Channel.IsPrivate = channelIsPrivate
	event.Data.Channel.RealExternalPlatformID = channelRealExternalPlatformId
	event.Data.Case.AuthorUser.ID = contactData.InboxAssigneeUser.Id
	event.Data.Case.AuthorUser.InContactID = contactData.InboxAssigneeUser.IncontactId
	event.Data.Case.AuthorUser.EmailAddress = contactData.InboxAssigneeUser.EmailAddress
	event.Data.Case.AuthorUser.LoginUsername = contactData.InboxAssigneeUser.LoginUsername
	event.Data.Case.AuthorUser.FirstName = contactData.InboxAssigneeUser.FirstName
	event.Data.Case.AuthorUser.SurName = contactData.InboxAssigneeUser.Surname
	event.Data.Case.AuthorEndUserIdentity.ID = contactData.AuthorEndUserIdentity.Id
	event.Data.Case.AuthorEndUserIdentity.IdOnExternalPlatform = contactData.AuthorEndUserIdentity.IdOnExternalPlatform
	event.Data.Case.AuthorEndUserIdentity.FullName = contactData.AuthorEndUserIdentity.FullName

	return event
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
		err = fmt.Errorf("[%s] non-shippable case update event - case or tenant id empty - case: %+v, tenantId: %s\n", makeCaseUpdateOp, message.Case, message.Brand.TenantID)
		return nil, err
	}

	return &message, nil
}

// getCreatedAt converts a date time to a custom pb timestamp
func getCreatedAt(event models.StreamEventRequest) (ts *timestamppb.Timestamp) {
	if event.CreatedAtWithMilliseconds != nil {
		return event.CreatedAtWithMilliseconds.Timestamp()
	}
	return event.CreatedAt.Timestamp()
}

// makeCase creates the streamEventRequest case message
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

// recipientMap turns the StreamEventRequest recipients list into the corresponding Recipients protobuf list.
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

// sendUpdateRecordsToMiddleware creates a gRPC client then pushes the createUpdateEvents to digimiddleware via gRPC
func sendUpdateRecordsToMiddleware(ctx context.Context, events *pbm.CaseUpdateEvents, dmwGrpcApiUrl string, log []byte) ([]byte, error) {
	var logMsg string
	var response *pbm.CaseResponse

	// Create gRPC client to pass CaseEventUpdate to digimiddleware
	logMsg = fmt.Sprintf("begin create grpc client: [%s]\n", createGrpcClientOp)
	log = createLog(logMsg, log)

	t := time.Now()
	middlewareEventService, err := createGrpcClient(dmwGrpcApiUrl)
	if err != nil {
		return log, err
	}

	if middlewareEventService == nil {
		err = fmt.Errorf("[%s] error creating middlewareEventService, error: %v", createGrpcClientOp, err)
		return log, err
	}

	logMsg = fmt.Sprintf("[%s] - grpc client initialized, duration=%s\n", createGrpcClientOp, time.Since(t))
	log = createLog(logMsg, log)

	// Push update records to dmw via grpc
	logMsg = fmt.Sprintf("begin grpc call to update records in digimiddleware: [%s]\n", caseEventUpdateOp)
	log = createLog(logMsg, log)

	t = time.Now()
	response, err = middlewareEventService.CaseEventUpdate(ctx, events)
	if err != nil {
		// Failure to deliver to Middleware (e.g., network errors, etc.)
		return log, err
	}

	errStr := response.GetErr()
	if errStr != "" {
		err = fmt.Errorf("[%s] received error from case update grpc: [%v]\n", caseEventUpdateOp, errStr)
		return log, err
	}

	logMsg = fmt.Sprintf("[%s] wrote case update records to grpc - records count: [%d], duration=%s\n", caseEventUpdateOp, len(events.Updates), time.Since(t))
	log = createLog(logMsg, log)

	return log, nil
}

// createCrpcClient creates the client needed to make gRPC calls to digimiddleware
func createGrpcClient(dmwGrpcApiUrl string) (digiservice.GrpcService, error) {
	// Extract environment name from dmwGrpcApiUrl
	env := extractEnvFromUrl(dmwGrpcApiUrl)
	serverName := extractServerNameFromUrl(dmwGrpcApiUrl)

	// Check if a certificate path is defined for the environment
	certPath, ok := certPaths[env]
	if !ok {
		return nil, fmt.Errorf("certificate path not found for environment: %s", env)
	}

	// Load the CA certificate for the environment
	caCert, err := ioutil.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %v", err)
	}

	// Create a certificate pool and add the CA certificate
	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caCert)

	// Create TLS credentials using the certificate pool
	creds := credentials.NewTLS(&tls.Config{
		ServerName:         serverName,
		RootCAs:            certPool,
		InsecureSkipVerify: true, // without this line, I am unable to run the tool in prod.  Until certificates include SANs, we need to keep this line here.
	})

	// Dial the gRPC server using TLS credentials
	conn, err := grpc.Dial(dmwGrpcApiUrl, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("initialization of digimiddleware gRPC client failed with error: %v", err)
	}

	middlewareEventService := digitransport.NewGRPCClient(conn, nil, nil)
	return middlewareEventService, nil
}

// extractEnvFromUrl extracts the environment name from URL
func extractEnvFromUrl(url string) string {
	// Split the URL by "."
	parts := strings.Split(url, ".")

	// The environment name is usually the last part of the URL
	return parts[len(parts)-2]
}

// extractServerNameFromUrl extracts the environment name from URL
func extractServerNameFromUrl(url string) string {
	// Split the URL by ":"
	parts := strings.Split(url, ":")

	// The environment name is usually the last part of the URL
	return parts[len(parts)-2]
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
