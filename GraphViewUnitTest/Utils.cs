using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;

namespace GraphViewUnitTest
{
    public static class DocDbUnitTestUtils
    {
        public static double stdDev(List<double> values)
        {
            double ret = 0;
            if (values.Count() > 0)
            {
                //Compute the Average      
                double avg = values.Average();
                //Perform the Sum of (value-avg)_2_2      
                double sum = values.Sum(d => Math.Pow(d - avg, 2));
                //Put it all together      
                ret = Math.Sqrt((sum) / (values.Count() - 1));
            }
            return ret;
        }

        //
        // [Obsolete] Use GraphViewConnection.ResetCollection() instead
        //
        //public static void ResetCollection(string collection)
        //{
        //    GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
        //            "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
        //            "GroupMatch", collection);

        //    connection.ResetCollection();
        //}
    }
}
