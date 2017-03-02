//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

using GraphView;

namespace GraphViewUnitTest.Gremlin
{
    using System;
    using System.Configuration;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides methods to generate the various sample TinkerPop graphs.
    /// </summary>
    public static class GraphDataLoader
    {
        /// <summary>
        /// Generates and Loads the correct Graph Db, on the local document Db instance.
        /// </summary>
        /// <param name="graphData">The type of graph data to load from among the TinkerPop samples.</param>
        public static void LoadGraphData(GraphData graphData)
        {
            switch (graphData)
            {
                case GraphData.CLASSIC:
                    LoadClassicGraphData();
                    break;
                case GraphData.MODERN:
                    LoadModernGraphData();
                    break;
                case GraphData.CREW:
                    throw new NotImplementedException("Crew requires supporting properties as documents themselves! This implementation currently does not support that functionality!!!");
                case GraphData.GRATEFUL:
                    throw new NotImplementedException("I'm not a fan of The Grateful Dead!");
                default:
                    throw new NotImplementedException("No idea how I ended up here!");
            }
        }

        /// <summary>
        /// Clears the Correct Graph on the local document Db instance, by clearing the appropriate collection.
        /// </summary>
        /// <param name="graphData">The type of graph data to clear from among the TinkerPop samples.</param>
        public static void ClearGraphData(GraphData graphData)
        {
            switch (graphData)
            {
                case GraphData.CLASSIC:
                    ClearGraphData(ConfigurationManager.AppSettings["DocDBCollectionClassic"]);
                    break;
                case GraphData.MODERN:
                    ClearGraphData(ConfigurationManager.AppSettings["DocDBCollectionModern"]);
                    break;
                case GraphData.CREW:
                    throw new NotImplementedException("Crew requires supporting properties as documents themselves! This implementation currently does not support that functionality!!!");
                case GraphData.GRATEFUL:
                    throw new NotImplementedException("I'm not a fan of The Grateful Dead!");
                default:
                    throw new NotImplementedException("No idea how I ended up here!");
            }
        }

        private static void LoadClassicGraphData()
        {
            GraphViewConnection connection = new GraphViewConnection(
                //ConfigurationManager.AppSettings["DocDBEndPoint"],
                ConfigurationManager.AppSettings["DocDBEndPointLocal"],
                //ConfigurationManager.AppSettings["DocDBKey"],
                ConfigurationManager.AppSettings["DocDBKeyLocal"],
                ConfigurationManager.AppSettings["DocDBDatabaseGremlin"],
                ConfigurationManager.AppSettings["DocDBCollectionClassic"]);
            connection.ResetCollection();

            GraphViewCommand graphCommand = new GraphViewCommand(connection);

            graphCommand.g().AddV("person").Property("name", "marko", "age", 29).Next();
            graphCommand.g().AddV("person").Property("name", "vadas", "age", 27).Next();
            graphCommand.g().AddV("software").Property("name", "lop", "lang", "java").Next();
            graphCommand.g().AddV("person").Property("name", "josh", "age", 32).Next();
            graphCommand.g().AddV("software").Property("name", "ripple", "lang", "java").Next();
            graphCommand.g().AddV("person").Property("name", "peter", "age", 35).Next();
            graphCommand.g().V().Has("name", "marko").AddE("knows").Property("weight", 0.5d).To(graphCommand.g().V().Has("name", "vadas")).Next();
            graphCommand.g().V().Has("name", "marko").AddE("knows").Property("weight", 1.0d).To(graphCommand.g().V().Has("name", "josh")).Next();
            graphCommand.g().V().Has("name", "marko").AddE("created").Property("weight", 0.4d).To(graphCommand.g().V().Has("name", "lop")).Next();
            graphCommand.g().V().Has("name", "josh").AddE("created").Property("weight", 1.0d).To(graphCommand.g().V().Has("name", "ripple")).Next();
            graphCommand.g().V().Has("name", "josh").AddE("created").Property("weight", 0.4d).To(graphCommand.g().V().Has("name", "lop")).Next();
            graphCommand.g().V().Has("name", "peter").AddE("created").Property("weight", 0.2d).To(graphCommand.g().V().Has("name", "lop")).Next();

            graphCommand.Dispose();
            connection.Dispose();
        }

        private static void LoadModernGraphData()
        {
            GraphViewConnection connection = new GraphViewConnection(
                //ConfigurationManager.AppSettings["DocDBEndPoint"],
                ConfigurationManager.AppSettings["DocDBEndPointLocal"],
                //ConfigurationManager.AppSettings["DocDBKey"],
                ConfigurationManager.AppSettings["DocDBKeyLocal"],
                ConfigurationManager.AppSettings["DocDBDatabaseGremlin"],
                ConfigurationManager.AppSettings["DocDBCollectionModern"]);
            connection.ResetCollection();

            GraphViewCommand graphCommand = new GraphViewCommand(connection);

            graphCommand.g().AddV("person").Property("name", "marko", "age", 29).Next();
            graphCommand.g().AddV("person").Property("name", "vadas", "age", 27).Next();
            graphCommand.g().AddV("software").Property("name", "lop", "lang", "java").Next();
            graphCommand.g().AddV("person").Property("name", "josh", "age", 32).Next();
            graphCommand.g().AddV("software").Property("name", "ripple", "lang", "java").Next();
            graphCommand.g().AddV("person").Property("name", "peter", "age", 35).Next();
            graphCommand.g().V().Has("name", "marko").AddE("knows").Property("weight", 0.5d).To(graphCommand.g().V().Has("name", "vadas")).Next();
            graphCommand.g().V().Has("name", "marko").AddE("knows").Property("weight", 1.0d).To(graphCommand.g().V().Has("name", "josh")).Next();
            graphCommand.g().V().Has("name", "marko").AddE("created").Property("weight", 0.4d).To(graphCommand.g().V().Has("name", "lop")).Next();
            graphCommand.g().V().Has("name", "josh").AddE("created").Property("weight", 1.0d).To(graphCommand.g().V().Has("name", "ripple")).Next();
            graphCommand.g().V().Has("name", "josh").AddE("created").Property("weight", 0.4d).To(graphCommand.g().V().Has("name", "lop")).Next();
            graphCommand.g().V().Has("name", "peter").AddE("created").Property("weight", 0.2d).To(graphCommand.g().V().Has("name", "lop")).Next();

            graphCommand.Dispose();
            connection.Dispose();
        }

        private static void ClearGraphData(string CollectionName)
        {
            GraphViewConnection connection = new GraphViewConnection(
                //ConfigurationManager.AppSettings["DocDBEndPoint"],
                ConfigurationManager.AppSettings["DocDBEndPointLocal"],
                //ConfigurationManager.AppSettings["DocDBKey"],
                ConfigurationManager.AppSettings["DocDBKeyLocal"],
                ConfigurationManager.AppSettings["DocDBDatabaseGremlin"],
                CollectionName);
            connection.ResetCollection();
            connection.Dispose();
        }
    }
}
