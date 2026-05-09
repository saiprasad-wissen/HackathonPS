public class OrderService {
    public Order createOrder(Order order) {
        // Reserve inventory first
        InventoryService inventoryService = new InventoryService();
        inventoryService.reserveInventory(order.getProducts());
        
        // Persist the order
        OrderRepository orderRepository = new OrderRepository();
        orderRepository.saveOrder(order);
        
        return order;
    }
}