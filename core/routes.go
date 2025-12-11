package core

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"text/tabwriter"

	"github.com/franz-kafka/server/core/handlers"
	"github.com/gorilla/mux"
)

type Routes struct {
	Router *mux.Router

	apiRoutePrefix string
	apiVersion     string
	opsRoutePrefix string

	tableWriter *tabwriter.Writer
}

func (r *Routes) registerOpsRoute(method, route string, handler func(http.ResponseWriter, *http.Request)) *Routes {
	completeRoute := "/" + r.opsRoutePrefix + route

	fmt.Fprintln(r.tableWriter, method+"\t "+completeRoute)

	r.Router.HandleFunc(completeRoute, handler).Methods(method)
	return r
}

func (r *Routes) registerApiRoute(method, route string, handler func(http.ResponseWriter, *http.Request)) *Routes {
	completeRoute := "/" + r.apiRoutePrefix + "/" + r.apiVersion + route

	fmt.Fprintln(r.tableWriter, method+"\t "+completeRoute)

	r.Router.HandleFunc(completeRoute, handler).Methods(method)
	return r
}

func NewRoutes(handler *handlers.Handler) *Routes {
	routes := &Routes{
		Router:         mux.NewRouter(),
		apiRoutePrefix: "api",
		apiVersion:     "v0",
		opsRoutePrefix: "ops",
		tableWriter:    tabwriter.NewWriter(os.Stdout, 5, 0, 3, ' ', tabwriter.Debug),
	}

	fmt.Fprintln(routes.tableWriter, "METHOD\t ROUTE")

	log.Println("Routes:")

	routes.
		registerOpsRoute("GET", "/health", handler.HealthCheck).
		registerOpsRoute("GET", "/config/dump", handler.ConfigDump).
		registerApiRoute("GET", "/cluster_claims", handler.ListClusterClaims).
		registerApiRoute("POST", "/cluster_claims", handler.CreateClusterClaim).
		registerApiRoute("GET", "/cluster_claim/{cluster_name}", handler.GetClusterClaim).
		registerApiRoute("PUT", "/cluster_claim/{cluster_name}", handler.UpdateClusterClaim).
		registerApiRoute("DELETE", "/cluster_claim/{cluster_name}", handler.DeleteClusterClaim)

	routes.tableWriter.Flush()

	return routes
}
