package models

type TenantWrapper struct {
	Tenant Tenant `json:"tenant"`
	Error  error
}

type Tenant struct {
	TenantId   string `json:"tenantId"`
	TenantName string `json:"tenantName"`
	Status     string `json:"status"`
	ClusterId  string `json:"clusterId"`
	BillingId  string `json:"billingId"`
}
