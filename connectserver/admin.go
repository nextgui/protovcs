package connectserver

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	pb "github.com/nextgui/protovcs/proto/protovcs/v1"
	"github.com/nextgui/protovcs/server"
)

// Admin is StoreAdminService's methods over a server, with no access check.
type Admin struct {
	server *server.Server
}

// NewAdmin returns the methods over srv.
func NewAdmin(srv *server.Server) (*Admin, error) {
	if srv == nil {
		return nil, errors.New("connectserver: built without a store server")
	}
	return &Admin{server: srv}, nil
}

// CreateStore records a new store and its limits.
func (a *Admin) CreateStore(ctx context.Context, req *pb.CreateStoreRequest) (*pb.CreateStoreResponse, error) {
	store, err := a.server.CreateStore(ctx, req)
	if err != nil {
		return nil, asConnect(err)
	}
	return &pb.CreateStoreResponse{Store: store}, nil
}

// GetStore returns one store's record.
func (a *Admin) GetStore(ctx context.Context, req *pb.GetStoreRequest) (*pb.GetStoreResponse, error) {
	store, err := a.server.GetStore(ctx, req.GetStoreId())
	if err != nil {
		return nil, asConnect(err)
	}
	return &pb.GetStoreResponse{Store: store}, nil
}

// ListStores answers operational questions about a fleet.
func (a *Admin) ListStores(ctx context.Context, req *pb.ListStoresRequest) (*pb.ListStoresResponse, error) {
	stores, err := a.server.ListStores(ctx, req)
	if err != nil {
		return nil, asConnect(err)
	}
	return &pb.ListStoresResponse{Stores: stores}, nil
}

// SetQuota replaces a store's limits.
func (a *Admin) SetQuota(ctx context.Context, req *pb.SetQuotaRequest) (*pb.SetQuotaResponse, error) {
	store, err := a.server.SetQuota(ctx, req.GetStoreId(), req.GetQuota())
	if err != nil {
		return nil, asConnect(err)
	}
	return &pb.SetQuotaResponse{Store: store}, nil
}

// GetUsage returns what a store is consuming.
func (a *Admin) GetUsage(ctx context.Context, req *pb.GetUsageRequest) (*pb.GetUsageResponse, error) {
	store, err := a.server.GetStore(ctx, req.GetStoreId())
	if err != nil {
		return nil, asConnect(err)
	}
	return &pb.GetUsageResponse{Usage: store.GetUsage()}, nil
}

// DeleteStore marks a store for deletion without removing anything.
func (a *Admin) DeleteStore(ctx context.Context, req *pb.DeleteStoreRequest) (*pb.DeleteStoreResponse, error) {
	store, err := a.server.DeleteStore(ctx, req.GetStoreId())
	if err != nil {
		return nil, asConnect(err)
	}
	return &pb.DeleteStoreResponse{Store: store}, nil
}

// RestoreStore undoes DeleteStore, while collection has not started purging
// the store.
func (a *Admin) RestoreStore(ctx context.Context, req *pb.RestoreStoreRequest) (*pb.RestoreStoreResponse, error) {
	store, err := a.server.RestoreStore(ctx, req.GetStoreId())
	if err != nil {
		return nil, asConnect(err)
	}
	return &pb.RestoreStoreResponse{Store: store}, nil
}

// TriggerMaintenance runs a job now rather than on the schedule.
func (a *Admin) TriggerMaintenance(ctx context.Context, req *pb.TriggerMaintenanceRequest) (*pb.TriggerMaintenanceResponse, error) {
	switch req.GetKind() {
	case pb.TriggerMaintenanceRequest_KIND_COLLECT:
		if _, err := a.server.Collect(ctx, req.GetStoreId()); err != nil {
			return nil, asConnect(err)
		}
	case pb.TriggerMaintenanceRequest_KIND_RECONCILE:
		if _, _, err := a.server.Reconcile(ctx, req.GetStoreId()); err != nil {
			return nil, asConnect(err)
		}
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("protovcs: TriggerMaintenance needs a kind"))
	}
	return &pb.TriggerMaintenanceResponse{}, nil
}
