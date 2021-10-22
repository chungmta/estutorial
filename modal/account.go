package modal

type Account struct {
	AccountNumber int
	Balance       int
	Firstname     string
	Lastname      string
	Age           int
	Gender        string
	Address       string
	Employer      string
	Email         string
	City          string
	State         string
}

func (Account) TableName() string {
	return "account"
}
