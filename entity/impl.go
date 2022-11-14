package entity

type impl struct {
	EntityID        string `json:"id"`
	EntityType      string `json:"type"`
	EntityVersion   uint64 `json:"version"`
	EntityState     string `json:"state"`
	EntityUpdatedBy string `json:"updatedBy"`
	EntityUpdatedAt string `json:"updatedAt"`
	EntityCreatedBy string `json:"createdBy"`
	EntityCreatedAt string `json:"createdAt"`
	EntityArchived  bool   `json:"archived"`
}
