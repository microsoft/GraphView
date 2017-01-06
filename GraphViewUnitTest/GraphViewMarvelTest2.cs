using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphViewUnitTest
{
    [TestClass]
    public class GraphViewMarvelTest2
    {
        [TestMethod]
        public void SelectMarvelQuery1()
        {
            GraphViewConnection connection = null;
            GraphTraversal2.SetGraphViewConnection(connection);
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            var result = GraphTraversal2.g().V().has("weapon", "shield").As("character").Out("appeared").As("comicbook").select("character").next();
        }

        [TestMethod]
        public void SelectMarvelQuery2()
        {
            GraphViewConnection connection = null;
            GraphTraversal2.SetGraphViewConnection(connection);
            var result =
                GraphTraversal2.g()
                    .V()
                    .has("weapon", "lasso")
                    .As("character")
                    .Out("appeared")
                    .As("comicbook")
                    .select("comicbook")
                    .next();
        }

        [TestMethod]
        public void SelectMarvelQuery3()
        {
            GraphViewConnection connection = null;
            GraphTraversal2.SetGraphViewConnection(connection);
            var result = GraphTraversal2.g().V().has("comicbook", "AVF 4").In("appeared").values("character").next();
        }

        [TestMethod]
        public void SelectMarvelQuery4()
        {
            GraphViewConnection connection = null;
            GraphTraversal2.SetGraphViewConnection(connection);
            var result = GraphTraversal2.g().V().has("comicbook", "AVF 4").In("appeared").has("weapon", "shield").values("character").next();
        }

        /// <summary>
        /// Print the characters and the comic-books they appeared in where the characters had a weapon that was a shield or claws.
        /// </summary>
        [TestMethod]
        public void SelectMarvelQueryNativeAPI1()
        {
            GraphViewConnection connection = null;
            GraphTraversal2.SetGraphViewConnection(connection);
            var result =
                GraphTraversal2.g().V()
                    .As("character")
                    .has("weapon", Predicate.within("shield", "claws"))
                    .Out("appeared")
                    .As("comicbook")
                    .select("character")
                    .next();
        }

        [TestMethod]
        public void SelectMarvelQueryNativeAPI2()
        {
            GraphViewConnection connection = null;
            GraphTraversal2.SetGraphViewConnection(connection);
            var result =
                GraphTraversal2.g().V()
                    .As("CharacterNode")
                    .values("character")
                    .As("character")
                    .@select("CharacterNode")
                    .has("weapon", Predicate.without("shield", "claws"))
                    .Out("appeared")
                    .values("comicbook")
                    .As("comicbook")
                    .select("comicbook")
                    .next();
        }

        [TestMethod]
        public void GraphViewMarvelInsertDeleteTest()
        {
            GraphViewConnection connection = null;
            GraphTraversal2.SetGraphViewConnection(connection);
            //GraphTraversal2.g().V().addV().property("character", "VENUS II").property("weapon", "shield").next();
            //GraphTraversal2.g().V().addV().property("comicbook", "AVF 4").next();
            GraphTraversal2.g().V().has("character", "VENUS II").addE().property("type", "appeared").to(GraphTraversal2.g().V().has("comicbook", "AVF 4")).next();
            GraphTraversal2.g().V().addV().property("character", "HAWK").property("weapon", "claws").next();
            GraphTraversal2.g().V().As("v").has("character", "HAWK").addE().property("type", "appeared").to(GraphTraversal2.g().V().has("comicbook", "AVF 4")).next();
            GraphTraversal2.g().V().addV().property("character", "WOODGOD").property("weapon", "lasso").next();
            GraphTraversal2.g().V().As("v").has("character", "WOODGOD").addE().property("type", "appeared").to(GraphTraversal2.g().V().has("comicbook", "AVF 4")).next();
        }
    }
}
