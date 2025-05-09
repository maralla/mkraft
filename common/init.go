package common

// to handle reliance of init manually
// uber has some framework to handle this automatically
func init() {
	InitLogger()
	InitConf()
}
