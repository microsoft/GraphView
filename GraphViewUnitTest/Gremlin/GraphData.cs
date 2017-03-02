//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace GraphViewUnitTest.Gremlin
{
    /// <summary>
    /// The various graph resource data sets that are available for use within the scope of this test project.
    /// </summary>
    public enum GraphData
    {
        /// <summary>
        /// Loads the "classic" TinkerPop toy graph.
        /// </summary>
        CLASSIC,

        /// <summary>
        /// Loads the "modern" TinkerPop toy graph which is like "classic", but with the "weight" value on edges stored as double and
        /// labels added for vertices. This should be the most commonly used graph instance for testing as graphs that support string,
        /// double, and integer should comprise the largest number of implementations.
        /// </summary>
        MODERN,

        /// <summary>
        /// Load "The Crew" TinkerPop3 toy graph which includes Vertex Property data.
        /// </summary>
        CREW,

        /// <summary>
        /// Loads the "grateful dead" graph which is a "large" graph which provides for the construction of more complex traversals.
        /// </summary>
        GRATEFUL
    }
}