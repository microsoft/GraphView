using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GraphViewAzureBatchUnitTest.Gremlin.Map
{
    [TestClass]
    public class GroupTest : AbstractAzureBatchGremlinTest
    {
        [TestMethod]
        public void g_V_Group()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                GraphViewCommand.OutputFormat = OutputFormat.Regular;
                this.job.Traversal = GraphViewCommand.g().V().Group();
                List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);

                foreach (var result in results)
                {
                    Console.WriteLine(result);
                }
            }
        }

        [TestMethod]
        public void g_V_Group_by()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                GraphViewCommand.OutputFormat = OutputFormat.Regular;
                this.job.Traversal = GraphViewCommand.g().V().Group().By(GraphTraversal.__().Values("name")).By();
                List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);

                foreach (var result in results)
                {
                    Console.WriteLine(result);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <remarks>
        /// bug: 
        /// In Out() step, records will be sent and received. 
        /// But in the process of rawrecord deserialization in receiveOp, VertexField.GetHashCode() will be invoked.
        /// However, the vertexField has nothing but searchInfo. I don't know why deserialize vertexField will invoke vertexField.GetHashCode.
        /// The error message is in the bottom.
        /// </remarks>
        [TestMethod]
        public void g_V_Group_by_select()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                GraphViewCommand.OutputFormat = OutputFormat.Regular;
                this.job.Traversal = GraphViewCommand.g().V().As("a").In().Select("a").GroupCount().Unfold().Select(GremlinKeyword.Column.Keys).Out().ValueMap();
                List<string> results = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);

                foreach (var result in results)
                {
                    Console.WriteLine(result);
                }
            }
        }

        [TestMethod]
        public void g_V_GroupCount()
        {
            using (GraphViewCommand GraphViewCommand = this.job.GetCommand())
            {
                this.job.Traversal = GraphViewCommand.g().V().GroupCount().Order(GremlinKeyword.Scope.Local).By(GremlinKeyword.Column.Values, GremlinKeyword.Order.Decr);
                List<string> result = StartAzureBatch.AzureBatchJobManager.TestQuery(this.job);
                foreach (var r in result)
                {
                    Console.WriteLine(r);
                }
            }
        }
    }
}

/*
 * Unhandled Exception: System.NullReferenceException: Object reference not set to an instance of an object.
   at GraphView.VertexField.get_Item(String propertyName)
   at GraphView.VertexField.GetHashCode()
   at GraphView.CompositeField.GetHashCode()
   at System.Collections.Generic.ObjectEqualityComparer`1.GetHashCode(T obj)
   at System.Collections.Generic.Dictionary`2.Insert(TKey key, TValue value, Boolean add)
   at ReadArrayOfKeyValueOfFieldObjectFieldObjectmwZEV_S8pFromXml(XmlReaderDelegator , XmlObjectSerializerReadContext , XmlDictionaryString , XmlDictionaryString , CollectionDataContract )
   at System.Runtime.Serialization.CollectionDataContract.ReadXmlValue(XmlReaderDelegator xmlReader, XmlObjectSerializerReadContext context)
   at System.Runtime.Serialization.XmlObjectSerializerReadContext.InternalDeserialize(XmlReaderDelegator reader, String name, String ns, Type declaredType, DataContract& dataContract)
   at System.Runtime.Serialization.XmlObjectSerializerReadContext.InternalDeserialize(XmlReaderDelegator xmlReader, Int32 id, RuntimeTypeHandle declaredTypeHandle, String name, String ns)
   at ReadMapFieldFromXml(XmlReaderDelegator , XmlObjectSerializerReadContext , XmlDictionaryString[] , XmlDictionaryString[] )
   at System.Runtime.Serialization.ClassDataContract.ReadXmlValue(XmlReaderDelegator xmlReader, XmlObjectSerializerReadContext context)
   at System.Runtime.Serialization.XmlObjectSerializerReadContext.InternalDeserialize(XmlReaderDelegator reader, String name, String ns, Type declaredType, DataContract& dataContract)
   at System.Runtime.Serialization.XmlObjectSerializerReadContext.InternalDeserialize(XmlReaderDelegator xmlReader, Int32 id, RuntimeTypeHandle declaredTypeHandle, String name, String ns)
   at ReadArrayOfFieldObjectFromXml(XmlReaderDelegator , XmlObjectSerializerReadContext , XmlDictionaryString , XmlDictionaryString , CollectionDataContract )
   at System.Runtime.Serialization.CollectionDataContract.ReadXmlValue(XmlReaderDelegator xmlReader, XmlObjectSerializerReadContext context)
   at System.Runtime.Serialization.XmlObjectSerializerReadContext.InternalDeserialize(XmlReaderDelegator reader, String name, String ns, Type declaredType, DataContract& dataContract)
   at System.Runtime.Serialization.XmlObjectSerializerReadContext.InternalDeserialize(XmlReaderDelegator xmlReader, Int32 id, RuntimeTypeHandle declaredTypeHandle, String name, String ns)
   at ReadRawRecordFromXml(XmlReaderDelegator , XmlObjectSerializerReadContext , XmlDictionaryString[] , XmlDictionaryString[] )
   at System.Runtime.Serialization.ClassDataContract.ReadXmlValue(XmlReaderDelegator xmlReader, XmlObjectSerializerReadContext context)
   at System.Runtime.Serialization.XmlObjectSerializerReadContext.InternalDeserialize(XmlReaderDelegator reader, String name, String ns, Type declaredType, DataContract& dataContract)
   at System.Runtime.Serialization.XmlObjectSerializerReadContext.InternalDeserialize(XmlReaderDelegator xmlReader, Int32 id, RuntimeTypeHandle declaredTypeHandle, String name, String ns)
   at ReadRawRecordMessageFromXml(XmlReaderDelegator , XmlObjectSerializerReadContext , XmlDictionaryString[] , XmlDictionaryString[] )
   at System.Runtime.Serialization.ClassDataContract.ReadXmlValue(XmlReaderDelegator xmlReader, XmlObjectSerializerReadContext context)
   at System.Runtime.Serialization.XmlObjectSerializerReadContext.InternalDeserialize(XmlReaderDelegator reader, String name, String ns, Type declaredType, DataContract& dataContract)
   at System.Runtime.Serialization.XmlObjectSerializerReadContext.InternalDeserialize(XmlReaderDelegator xmlReader, Type declaredType, DataContract dataContract, String name, String ns)
   at System.Runtime.Serialization.DataContractSerializer.InternalReadObject(XmlReaderDelegator xmlReader, Boolean verifyObjectName, DataContractResolver dataContractResolver)
   at System.Runtime.Serialization.XmlObjectSerializer.ReadObjectHandleExceptions(XmlReaderDelegator reader, Boolean verifyObjectName, DataContractResolver dataContractResolver)
   at System.Runtime.Serialization.XmlObjectSerializer.ReadObject(XmlDictionaryReader reader)
   at GraphView.RawRecordMessage.DecodeMessage(String message, GraphViewCommand command)
   at GraphView.ReceiveOperator.Next()
   at GraphView.TraversalOperator.Next()
   at GraphView.TableValuedFunction.Next()
   at GraphView.ProjectOperator.Next()
   at GraphView.GraphTraversal.ExecuteQueryByDeserialization(String serializationStr, String partitionStr)
   at GraphViewProgram.Program.Main(String[] args)
 */
