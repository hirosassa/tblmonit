package config

type Config struct {
	Project []Project
}

type Project struct {
	Name    string
	Dataset []Dataset
}

type Dataset struct {
	Name string
	TableConfig []TableConfig
}

type TableConfig struct {
	Table         string
	DateForShards string
	Timethreshold string
}

func Expand(config Config) {

}
