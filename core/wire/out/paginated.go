package out

type Paginated struct {
	Total    int         `json:"total"`
	Page     int         `json:"page,omitempty"`
	PageSize int         `json:"page_size,omitempty"`
	Items    interface{} `json:"items"`
}
