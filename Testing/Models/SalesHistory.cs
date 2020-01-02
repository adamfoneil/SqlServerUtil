using System;

namespace Testing.Models
{
    public class SalesHistory
    {
        public string Customer { get; set; }
        public string Region { get; set; }
        public DateTime Date { get; set; }
        public string ItemNumber { get; set; }
        public int Quantity { get; set; }
        public decimal Price { get; set; }
        public int Id { get; set; }
    }
}
