from hummingbot.connector.in_flight_order_base cimport InFlightOrderBase

cdef class VitexInFlightOrder(InFlightOrderBase):
    cdef:
        public object trade_id_set
        public object execute_price
