package models

type ChannelData struct {
	Id                     string `json:"id"`
	ChannelId              string `json:"channelId"`
	IdOnExternalPlatform   string `json:"idOnExternalPlatform"`
	RealExternalPlatformId string `json:"realExternalPlatformId"`
	Name                   string `json:"name"`
	IntegrationBoxIdent    string `json:"integrationBoxIdent"`
	IsPrivate              bool   `json:"isPrivate"`
	IsDeleted              bool   `json:"isDeleted"`
}

// ChannelSort sorts by ChannelId
type ChannelSort []ChannelData

func (a ChannelSort) Len() int { return len(a) }
func (a ChannelSort) Less(i, j int) bool {
	return a[i].ChannelId < a[j].ChannelId
}

func (a ChannelSort) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
