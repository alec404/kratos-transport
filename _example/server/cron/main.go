package main

import (
	"time"

	crontransport "github.com/alec404/kratos-transport/transport/cron"
	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
)

func main() {
	cronSrv := crontransport.NewServer()

	_, err := cronSrv.StartTimerJob("@every 5s", func() {
		log.Infof("cron task fired at %s", time.Now().Format(time.RFC3339))
	})
	if err != nil {
		log.Fatal(err)
	}

	app := kratos.New(
		kratos.Name("cron-example"),
		kratos.Server(cronSrv),
	)
	if err := app.Run(); err != nil {
		log.Error(err)
	}
}
