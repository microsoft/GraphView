﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal enum EdgeDir : byte { In, Out };

    internal enum MaterializedOrder { Pre, Post };

    internal class OneHeightTree
    {
        public MatchNode TreeRoot { get; set; }
        public List<MatchEdge> Edges { get; set; }
    }

    /// <summary>
    /// A 1-height tree is a node with one or more outgoing edges. 
    /// </summary>
    internal class CandidateJoinUnit
    {
        public MatchNode TreeRoot { get; set; }

        // Incoming edges being transposed before Join
        public List<MatchEdge> PreMatIncomingEdges { get; set; }
        // Outgoing edges being transposed before Join
        public List<MatchEdge> PreMatOutgoingEdges { get; set; }
        // Incoming edges being transposed after Join
        public List<MatchEdge> PostMatIncomingEdges { get; set; }
        // Outgoing edges being transposed after Join
        public List<MatchEdge> PostMatOutgoingEdges { get; set; }

        // TreeRoot's edges that are not yet materialized. Lazy materialization may be beneficial for performance, 
        // because it reduces the number of intermediate resutls. 
        public List<MatchEdge> UnmaterializedEdges { get; set; }
    }

    /// <summary>
    /// The Component in the joining process
    /// </summary>
    internal class MatchComponent
    {
        public HashSet<MatchNode> Nodes { get; set; }
        public Dictionary<MatchEdge, bool> EdgeMaterilizedDict { get; set; }
        // Stores the split count of a materialized node
        public Dictionary<MatchNode, int> MaterializedNodeSplitCount { get; set; }

        public List<Tuple<MatchNode, MatchEdge>> TraversalChain { get; set; }

        public int ActiveNodeCount
        {
            get { return MaterializedNodeSplitCount.Count; }
        }

        // Maps the unmaterialized node to the alias of one of its incoming materialized edges;
        // the join condition between the node and the incoming edge should be added 
        // when the node is materialized.
        public Dictionary<MatchNode, List<MatchEdge>> UnmaterializedNodeMapping { get; set; }

        // A collection of sink nodes and their statistics.
        // A sink node's statistic will be updated as new candidates are added to the component
        // and new edges point to this sink node. 
        public Dictionary<MatchNode, Statistics> SinkNodeStatisticsDict { get; set; }

        // Estimated number of rows returned by this component
        public double Cardinality { get; set; }

        public double Cost { get; set; }

        public MatchComponent()
        {
            Nodes = new HashSet<MatchNode>();
            EdgeMaterilizedDict = new Dictionary<MatchEdge, bool>();
            MaterializedNodeSplitCount = new Dictionary<MatchNode, int>();
            TraversalChain = new List<Tuple<MatchNode, MatchEdge>>();
            UnmaterializedNodeMapping = new Dictionary<MatchNode, List<MatchEdge>>();
            SinkNodeStatisticsDict = new Dictionary<MatchNode, Statistics>();
            Cardinality = 1.0;
            Cost = 0.0;
        }

        public MatchComponent(MatchNode node) : this()
        {
            Nodes.Add(node);
            MaterializedNodeSplitCount[node] = 0;
            //SinkNodeStatisticsDict[node] = new Statistics ();
            Cardinality *= node.EstimatedRows;

            foreach (var edge in node.Neighbors)
            {
                var edgeList = UnmaterializedNodeMapping.GetOrCreate(edge.SinkNode);
                edgeList.Add(edge);
            }
        }

        /// <summary>
        /// Deep Copy
        /// </summary>
        /// <param name="component"></param>
        public MatchComponent(MatchComponent component)
        {
            Nodes = new HashSet<MatchNode>(component.Nodes);
            EdgeMaterilizedDict = new Dictionary<MatchEdge, bool>(component.EdgeMaterilizedDict);
            MaterializedNodeSplitCount = new Dictionary<MatchNode, int>(component.MaterializedNodeSplitCount);
            UnmaterializedNodeMapping = new Dictionary<MatchNode, List<MatchEdge>>();
            foreach (var nodeMapping in component.UnmaterializedNodeMapping)
            {
                UnmaterializedNodeMapping[nodeMapping.Key] = new List<MatchEdge>(nodeMapping.Value);
            }
            TraversalChain = new List<Tuple<MatchNode, MatchEdge>>();
            foreach (var chain in component.TraversalChain)
            {
                TraversalChain.Add(new Tuple<MatchNode, MatchEdge>(chain.Item1, chain.Item2));
            }
            SinkNodeStatisticsDict = new Dictionary<MatchNode, Statistics>(component.SinkNodeStatisticsDict);
            Cardinality = component.Cardinality;
            Cost = component.Cost;
        }
    }
}
