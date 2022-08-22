package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
)

//Dynamo has a TTL defined in Dynamo persister in dmw (a.Flag("persister.ttl", "ttl in seconds for database records.  ttl is set when case is closed or trashed.").Default("21600").IntVar(&cfg.persisterTTL))

//Set up an HttpClient
//For DynamoDB call, we do not need authentication
//For DFO 3.0 call, we need Bearer Token using the golden key (dev presence sync lambda)
//Create Auth
//Use DFO 3.0 that will allow responses for multiple tenantIds??
//To find the an auth token, log in to CXOne for that env/region (dev, test, staging, prod).  Go to DEV tools, Application, Local Storage, na1.dev.nice..., copy the userToken.
//Consider running this from the console and token being a command line parameter until we can get a T0 service user.

type TenantIdsObj struct {
	TenantIDs [1]string `json:"tenantIds"`
}

type DmwKnownContacts struct {
	Contacts []DmwKnownContact `json:"contacts"`
}

type DmwKnownContact struct {
	ContactID           int64             `json:"ContactID"`
	MasterContactID     int64             `json:"MasterContactID"`
	TenantID            string            `json:"TenantID"`
	StartDate           string			  `json:"StartDate"`
	FromAddr            string            `json:"FromAddr"`
	CurrentContactState int32             `json:"CurrentContactState"`
	CurrentContactDate  string			  `json:"CurrentContactDate"`
	Direction           int32             `json:"Direction"`
	ChannelID           string            `json:"ChannelID"`
	StateIndex          int32             `json:"StateIndex"`
	CaseIDString        string            `json:"CaseIDString"`
	DigitalContactState int32             `json:"DigitalContactState"`
	EventID             string            `json:"EventID"`
}

type DfoActiveContacts struct {
	Data []DfoActiveContact `json:"data"`
}

type DfoActiveContact struct {
	Id string `json:"id"`
	Status string `json:"status"`
	RoutingQueueId string `json:"routingQueueId"`
	OwnerAssigneeUser OwnerAssigneeUser `json:"ownerAssigneeUser,omitempty"`
}

type OwnerAssigneeUser struct {
	Id int32 `json:"id"`
	IncontactId string `json:"incontactId,omitempty"`
	EmailAddress string `json:"emailaddress,omitempty"`
	LoginUsername string `json:"loginUsername,omitempty"`
	FirstName string `json:"firstName,omitempty"`
	Surname string `json:"surname,omitempty"`
	Nickname string `json:"nickname,omitempty"`
	ImageUrl string `json:"imageUrl,omitempty"`
	PublicImageUrl string `json:"publicImageUrl,omitempty"`
	IsBotUser bool `json:"isBotUser,omitempty"`
	IsSurveyUser bool `json:"isSurveyUser,omitempty"`
}

func main() {
	// TODO: Call get tenants to get list of tenants that DMW is aware of
	// TODO: Loop through each tenant to get the active contacts, compare, and then update DMW contact states
	// Get list of Digimiddleware known active contacts
	dmwGetContactStateApiUrl := "http://digi-shared-eks01-na1.omnichannel.dev.internal:8085/digimiddleware/getstatesbytenants"
	dmwResponse := GetDmwActiveContactStateData(dmwGetContactStateApiUrl)

	var dmwKnownContacts DmwKnownContacts
	if dmwResponse != nil {
		err := json.Unmarshal(dmwResponse, &dmwKnownContacts)
		if err != nil {
			fmt.Println("Cannot unmarshal dmwResponse")
		}
	}

	// Get list of DFO active contacts
	dfoGetActiveContactsApiUrl := "https://api-de-na1.dev.niceincontact.com/dfo/3.0/contacts?status[]=new&status[]=resolved&status[]=escalated&status[]=pending&status[]=open"
	dfoResponse := GetDfoActiveContactList(dfoGetActiveContactsApiUrl)

	var dfoActiveContacts DfoActiveContacts
	if dfoResponse != nil {
		err := json.Unmarshal(dfoResponse, &dfoActiveContacts)
		if err != nil {
			fmt.Println("Cannot unmarshal dfoResponse")
		}
	}

	// Compare lists to get the contacts that exist in DMW but are closed in DFO.
	deltaList := GetDeltaList(dmwKnownContacts, dfoActiveContacts)
	fmt.Println(deltaList)

	//TODO: Update each contact in the deltaList to set state to closed
	//TODO: Call api to update DMW contact states to set contact to closed.


}

func GetDeltaList(dmwKnownContacts DmwKnownContacts, dfoActiveContacts DfoActiveContacts) []byte {
	found := false
	type DmwKnownContacts []DmwKnownContact
	deltaList := DmwKnownContacts{}

	// Loop through DmwKnownContacts and check each contact data in DfoActiveContacts to see if we find a match
	for _, contact := range dmwKnownContacts.Contacts {
		for _, data := range dfoActiveContacts.Data {
			dataId, _ := strconv.ParseInt(data.Id, 10, 64)
			if contact.ContactID == dataId {
				// TODO: Compare the states - if contact found but states not matched, then we need to update.
				found = true
				break
			}
		}

		// If no match is found in DfoActiveContacts, add contact data to deltaList
		if !found {
			fmt.Printf("Adding ContactID: [%v] to deltaList\n", contact.ContactID)

			delta := DmwKnownContact{
				ContactID: contact.ContactID,
				MasterContactID: contact.MasterContactID,
				TenantID: contact.TenantID,
				StartDate: contact.StartDate,
				FromAddr: contact.FromAddr,
				CurrentContactState: contact.CurrentContactState,
				CurrentContactDate: contact.CurrentContactDate,
				Direction: contact.Direction,
				ChannelID: contact.ChannelID,
				StateIndex: contact.StateIndex,
				CaseIDString: contact.CaseIDString,
				DigitalContactState: contact.DigitalContactState,
				EventID: contact.EventID,
			}

			deltaList = append(deltaList, delta)
		}
	}

	reqBodyBytes := new(bytes.Buffer)
	json.NewEncoder(reqBodyBytes).Encode(deltaList)
	return reqBodyBytes.Bytes()
}

func GetDmwActiveContactStateData(apiUrl string) []byte {
	contentType := "application/json"
	var tenants [1]string
	tenants[0] = "11EB505F-7844-7680-923B-0242AC110003" //15572	perm_DFI_OSH_DO74
	var tenantIdsObj TenantIdsObj
	tenantIdsObj.TenantIDs = tenants
	tenantJson, _ := json.Marshal(tenantIdsObj)
	reader := bytes.NewReader(tenantJson)
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

func GetDfoActiveContactList(apiUrl string) []byte {

	var responseData []byte

	// TODO: check auth token to see if it is no longer active
	// TODO: make call to get new auth token
	// Create a Bearer string by appending string access token
	var bearer = "Bearer " + "3067c16d5ae0c950029e397bc0d07afc4ef9442e"

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

//var tenantArr [...]string
//tenantArr[0] = "11EB505F-7844-7680-923B-0242AC110003" //15572	perm_DFI_OSH_DO74
//tenantArr[1] = "11EB664D-C2B5-EE70-8733-0242AC110002" //15576	perm_DFI-BillingcycleStart8
//tenantArr[2] = "11EB664E-03B7-9FE0-8733-0242AC110002" //15577	perm_DFI_BillingCycleStart23
