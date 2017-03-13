
namespace GraphView
{
    /// <summary>
    /// The type of the collection provision to run DocumentDB graph Engine
    /// </summary>
    public enum CollectionType
    {
        /// <summary>
        /// Standard non-partitioned collection
        /// </summary>
        STANDARD,

        /// <summary>
        /// Partitioned collection
        /// </summary>
        PARTITIONED,

        /// <summary>
        /// Type not provided
        /// </summary>
        UNDEFINED
    }
}
