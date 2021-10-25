package modal

type Account struct {
	AccountNumber int    `json:"account_number"`
	Balance       int    `json:"balance"`
	Firstname     string `json:"firstname"`
	Lastname      string `json:"lastname"`
	Age           int    `json:"age"`
	Gender        string `json:"gender"`
	Address       string `json:"address"`
	Employer      string `json:"employer"`
	Email         string `json:"email"`
	City          string `json:"city"`
	State         string `json:"state"`
}

func (Account) TableName() string {
	return "account"
}
