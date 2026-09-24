package http

import (
	"encoding/json"
	"net/http"

	"github.com/Nikhil-O1O5/kafka-go/internal/service"
	"github.com/sirupsen/logrus"
)

type OrderHandler struct {
	orderService *service.OrderService
}

func NewOrderHandler(orderService *service.OrderService) *OrderHandler {
	return &OrderHandler{orderService: orderService}
}

func (h *OrderHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /orders", h.createOrder)
}

type createOrderRequest struct {
	Item string `json:"item"`
}

type createOrderResponse struct {
	OrderId string `json:"order_id"`
}

func (h *OrderHandler) createOrder(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Item == "" {
		http.Error(w, "item is required", http.StatusBadRequest)
		return
	}

	orderId, err := h.orderService.Create(r.Context(), req.Item)
	if err != nil {
		logrus.WithError(err).Error("create order failed")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(createOrderResponse{OrderId: orderId})
}
