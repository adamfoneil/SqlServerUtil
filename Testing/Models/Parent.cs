namespace Testing.Models
{
    public class Parent
    {
        public string Name { get; set; }
        public int Id { get; set; }
    }

    public class Child
    {
        public int ParentId { get; set; }
        public string Name { get; set; }
        public int Id { get; set; }
    }
}
