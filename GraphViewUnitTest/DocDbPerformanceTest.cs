using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using GraphView;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Linq;

namespace GraphViewUnitTest
{
    [TestClass]
    public class PerformanceTest
    {
        int count;
        internal async Task<Exception> IN()
        {
            try
            {
                GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

                GraphViewCommand gcmd = new GraphViewCommand();
                gcmd.GraphViewConnection = connection;

                connection.SetupClient();

                #region InsertNode Text

                gcmd.CommandText = @"
INSERT INTO Node (name, age, type) VALUES ('node1', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node2', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node3', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node4', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node5', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node6', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node7', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node8', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node9', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node10', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node11', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node12', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node13', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node14', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node15', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node16', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node17', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node18', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node19', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node20', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node21', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node22', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node23', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node24', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node25', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node26', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node27', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node28', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node29', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node30', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node31', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node32', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node33', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node34', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node35', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node36', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node37', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node38', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node39', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node40', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node41', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node42', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node43', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node44', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node45', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node46', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node47', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node48', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node49', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node50', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node51', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node52', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node53', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node54', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node55', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node56', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node57', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node58', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node59', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node60', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node61', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node62', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node63', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node64', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node65', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node66', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node67', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node68', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node69', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node70', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node71', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node72', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node73', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node74', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node75', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node76', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node77', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node78', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node79', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node80', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node81', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node82', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node83', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node84', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node85', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node86', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node87', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node88', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node89', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node90', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node91', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node92', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node93', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node94', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node95', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node96', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node97', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node98', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node99', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node100', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node101', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node102', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node103', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node104', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node105', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node106', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node107', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node108', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node109', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node110', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node111', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node112', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node113', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node114', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node115', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node116', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node117', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node118', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node119', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node120', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node121', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node122', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node123', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node124', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node125', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node126', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node127', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node128', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node129', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node130', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node131', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node132', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node133', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node134', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node135', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node136', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node137', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node138', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node139', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node140', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node141', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node142', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node143', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node144', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node145', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node146', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node147', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node148', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node149', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node150', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node151', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node152', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node153', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node154', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node155', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node156', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node157', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node158', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node159', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node160', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node161', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node162', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node163', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node164', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node165', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node166', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node167', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node168', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node169', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node170', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node171', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node172', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node173', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node174', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node175', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node176', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node177', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node178', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node179', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node180', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node181', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node182', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node183', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node184', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node185', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node186', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node187', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node188', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node189', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node190', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node191', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node192', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node193', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node194', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node195', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node196', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node197', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node198', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node199', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node200', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node201', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node202', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node203', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node204', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node205', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node206', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node207', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node208', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node209', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node210', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node211', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node212', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node213', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node214', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node215', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node216', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node217', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node218', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node219', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node220', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node221', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node222', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node223', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node224', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node225', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node226', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node227', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node228', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node229', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node230', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node231', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node232', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node233', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node234', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node235', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node236', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node237', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node238', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node239', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node240', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node241', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node242', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node243', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node244', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node245', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node246', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node247', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node248', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node249', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node250', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node251', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node252', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node253', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node254', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node255', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node256', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node257', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node258', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node259', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node260', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node261', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node262', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node263', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node264', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node265', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node266', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node267', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node268', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node269', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node270', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node271', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node272', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node273', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node274', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node275', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node276', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node277', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node278', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node279', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node280', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node281', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node282', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node283', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node284', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node285', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node286', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node287', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node288', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node289', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node290', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node291', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node292', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node293', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node294', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node295', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node296', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node297', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node298', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node299', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node300', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node301', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node302', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node303', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node304', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node305', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node306', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node307', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node308', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node309', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node310', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node311', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node312', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node313', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node314', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node315', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node316', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node317', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node318', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node319', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node320', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node321', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node322', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node323', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node324', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node325', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node326', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node327', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node328', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node329', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node330', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node331', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node332', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node333', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node334', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node335', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node336', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node337', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node338', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node339', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node340', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node341', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node342', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node343', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node344', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node345', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node346', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node347', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node348', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node349', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node350', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node351', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node352', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node353', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node354', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node355', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node356', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node357', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node358', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node359', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node360', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node361', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node362', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node363', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node364', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node365', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node366', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node367', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node368', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node369', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node370', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node371', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node372', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node373', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node374', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node375', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node376', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node377', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node378', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node379', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node380', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node381', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node382', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node383', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node384', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node385', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node386', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node387', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node388', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node389', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node390', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node391', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node392', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node393', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node394', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node395', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node396', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node397', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node398', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node399', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node400', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node401', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node402', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node403', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node404', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node405', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node406', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node407', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node408', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node409', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node410', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node411', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node412', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node413', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node414', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node415', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node416', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node417', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node418', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node419', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node420', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node421', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node422', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node423', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node424', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node425', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node426', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node427', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node428', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node429', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node430', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node431', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node432', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node433', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node434', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node435', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node436', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node437', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node438', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node439', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node440', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node441', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node442', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node443', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node444', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node445', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node446', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node447', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node448', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node449', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node450', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node451', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node452', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node453', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node454', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node455', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node456', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node457', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node458', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node459', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node460', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node461', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node462', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node463', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node464', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node465', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node466', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node467', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node468', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node469', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node470', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node471', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node472', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node473', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node474', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node475', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node476', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node477', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node478', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node479', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node480', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node481', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node482', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node483', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node484', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node485', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node486', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node487', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node488', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node489', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node490', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node491', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node492', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node493', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node494', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node495', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node496', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node497', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node498', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node499', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node500', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node501', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node502', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node503', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node504', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node505', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node506', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node507', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node508', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node509', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node510', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node511', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node512', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node513', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node514', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node515', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node516', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node517', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node518', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node519', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node520', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node521', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node522', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node523', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node524', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node525', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node526', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node527', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node528', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node529', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node530', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node531', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node532', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node533', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node534', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node535', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node536', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node537', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node538', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node539', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node540', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node541', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node542', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node543', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node544', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node545', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node546', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node547', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node548', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node549', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node550', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node551', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node552', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node553', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node554', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node555', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node556', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node557', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node558', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node559', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node560', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node561', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node562', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node563', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node564', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node565', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node566', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node567', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node568', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node569', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node570', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node571', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node572', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node573', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node574', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node575', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node576', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node577', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node578', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node579', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node580', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node581', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node582', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node583', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node584', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node585', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node586', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node587', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node588', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node589', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node590', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node591', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node592', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node593', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node594', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node595', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node596', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node597', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node598', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node599', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node600', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node601', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node602', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node603', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node604', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node605', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node606', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node607', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node608', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node609', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node610', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node611', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node612', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node613', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node614', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node615', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node616', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node617', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node618', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node619', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node620', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node621', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node622', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node623', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node624', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node625', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node626', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node627', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node628', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node629', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node630', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node631', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node632', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node633', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node634', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node635', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node636', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node637', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node638', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node639', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node640', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node641', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node642', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node643', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node644', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node645', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node646', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node647', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node648', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node649', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node650', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node651', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node652', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node653', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node654', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node655', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node656', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node657', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node658', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node659', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node660', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node661', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node662', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node663', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node664', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node665', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node666', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node667', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node668', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node669', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node670', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node671', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node672', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node673', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node674', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node675', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node676', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node677', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node678', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node679', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node680', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node681', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node682', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node683', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node684', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node685', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node686', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node687', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node688', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node689', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node690', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node691', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node692', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node693', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node694', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node695', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node696', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node697', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node698', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node699', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node700', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node701', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node702', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node703', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node704', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node705', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node706', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node707', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node708', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node709', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node710', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node711', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node712', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node713', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node714', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node715', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node716', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node717', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node718', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node719', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node720', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node721', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node722', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node723', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node724', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node725', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node726', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node727', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node728', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node729', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node730', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node731', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node732', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node733', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node734', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node735', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node736', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node737', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node738', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node739', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node740', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node741', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node742', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node743', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node744', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node745', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node746', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node747', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node748', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node749', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node750', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node751', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node752', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node753', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node754', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node755', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node756', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node757', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node758', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node759', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node760', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node761', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node762', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node763', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node764', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node765', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node766', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node767', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node768', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node769', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node770', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node771', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node772', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node773', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node774', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node775', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node776', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node777', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node778', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node779', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node780', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node781', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node782', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node783', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node784', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node785', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node786', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node787', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node788', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node789', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node790', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node791', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node792', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node793', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node794', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node795', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node796', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node797', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node798', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node799', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node800', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node801', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node802', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node803', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node804', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node805', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node806', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node807', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node808', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node809', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node810', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node811', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node812', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node813', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node814', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node815', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node816', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node817', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node818', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node819', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node820', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node821', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node822', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node823', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node824', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node825', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node826', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node827', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node828', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node829', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node830', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node831', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node832', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node833', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node834', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node835', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node836', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node837', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node838', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node839', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node840', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node841', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node842', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node843', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node844', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node845', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node846', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node847', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node848', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node849', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node850', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node851', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node852', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node853', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node854', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node855', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node856', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node857', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node858', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node859', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node860', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node861', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node862', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node863', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node864', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node865', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node866', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node867', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node868', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node869', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node870', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node871', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node872', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node873', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node874', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node875', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node876', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node877', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node878', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node879', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node880', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node881', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node882', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node883', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node884', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node885', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node886', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node887', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node888', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node889', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node890', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node891', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node892', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node893', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node894', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node895', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node896', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node897', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node898', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node899', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node900', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node901', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node902', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node903', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node904', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node905', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node906', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node907', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node908', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node909', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node910', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node911', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node912', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node913', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node914', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node915', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node916', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node917', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node918', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node919', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node920', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node921', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node922', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node923', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node924', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node925', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node926', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node927', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node928', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node929', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node930', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node931', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node932', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node933', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node934', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node935', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node936', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node937', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node938', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node939', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node940', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node941', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node942', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node943', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node944', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node945', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node946', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node947', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node948', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node949', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node950', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node951', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node952', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node953', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node954', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node955', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node956', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node957', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node958', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node959', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node960', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node961', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node962', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node963', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node964', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node965', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node966', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node967', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node968', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node969', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node970', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node971', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node972', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node973', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node974', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node975', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node976', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node977', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node978', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node979', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node980', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node981', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node982', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node983', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node984', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node985', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node986', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node987', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node988', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node989', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node990', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node991', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node992', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node993', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node994', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node995', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node996', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node997', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node998', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node999', 10, 'human');
INSERT INTO Node (name, age, type) VALUES ('node1000', 10, 'human');

";

                #endregion

                Debug.WriteLine(string.Format("start ,   count = {0}", count));

                await Task.Delay(1);
                gcmd.ExecuteNonQuery();

                count++;

                Debug.WriteLine(string.Format("end   ,   count = {0}", count));
                
                return new Exception("none");
            }
            catch (Exception documentClientException)
            {
                return documentClientException;
            }
        }
        [TestMethod]
        public void InsertNode()
        {


            count = 0;

            IN();

            while (count != 1)
                System.Threading.Thread.Sleep(10);
            

            //connection.ResetCollection();
        }


        [TestMethod]
        public void ResetCollection()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");
            connection.SetupClient();

            connection.DocDB_finish = false;
            connection.BuildUp();
            while (!connection.DocDB_finish)
                System.Threading.Thread.Sleep(10);

            connection.ResetCollection();

            connection.DocDB_finish = false;
            connection.BuildUp();
            while (!connection.DocDB_finish)
                System.Threading.Thread.Sleep(10);
        }

        [TestMethod]
        public void InsertEdge()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", "GraphTest");

            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            connection.SetupClient();
            #region InsertEdge Text
            gcmd.CommandText = @"

INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node1' AND B.name = 'node2'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node2' AND B.name = 'node3'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node3' AND B.name = 'node4'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node4' AND B.name = 'node5'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node5' AND B.name = 'node6'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node6' AND B.name = 'node7'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node7' AND B.name = 'node8'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node8' AND B.name = 'node9'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node9' AND B.name = 'node10'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node10' AND B.name = 'node11'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node11' AND B.name = 'node12'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node12' AND B.name = 'node13'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node13' AND B.name = 'node14'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node14' AND B.name = 'node15'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node15' AND B.name = 'node16'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node16' AND B.name = 'node17'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node17' AND B.name = 'node18'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node18' AND B.name = 'node19'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node19' AND B.name = 'node20'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node20' AND B.name = 'node21'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node21' AND B.name = 'node22'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node22' AND B.name = 'node23'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node23' AND B.name = 'node24'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node24' AND B.name = 'node25'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node25' AND B.name = 'node26'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node26' AND B.name = 'node27'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node27' AND B.name = 'node28'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node28' AND B.name = 'node29'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node29' AND B.name = 'node30'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node30' AND B.name = 'node31'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node31' AND B.name = 'node32'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node32' AND B.name = 'node33'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node33' AND B.name = 'node34'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node34' AND B.name = 'node35'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node35' AND B.name = 'node36'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node36' AND B.name = 'node37'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node37' AND B.name = 'node38'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node38' AND B.name = 'node39'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node39' AND B.name = 'node40'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node40' AND B.name = 'node41'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node41' AND B.name = 'node42'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node42' AND B.name = 'node43'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node43' AND B.name = 'node44'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node44' AND B.name = 'node45'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node45' AND B.name = 'node46'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node46' AND B.name = 'node47'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node47' AND B.name = 'node48'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node48' AND B.name = 'node49'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node49' AND B.name = 'node50'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node50' AND B.name = 'node51'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node51' AND B.name = 'node52'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node52' AND B.name = 'node53'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node53' AND B.name = 'node54'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node54' AND B.name = 'node55'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node55' AND B.name = 'node56'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node56' AND B.name = 'node57'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node57' AND B.name = 'node58'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node58' AND B.name = 'node59'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node59' AND B.name = 'node60'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node60' AND B.name = 'node61'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node61' AND B.name = 'node62'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node62' AND B.name = 'node63'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node63' AND B.name = 'node64'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node64' AND B.name = 'node65'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node65' AND B.name = 'node66'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node66' AND B.name = 'node67'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node67' AND B.name = 'node68'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node68' AND B.name = 'node69'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node69' AND B.name = 'node70'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node70' AND B.name = 'node71'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node71' AND B.name = 'node72'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node72' AND B.name = 'node73'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node73' AND B.name = 'node74'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node74' AND B.name = 'node75'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node75' AND B.name = 'node76'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node76' AND B.name = 'node77'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node77' AND B.name = 'node78'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node78' AND B.name = 'node79'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node79' AND B.name = 'node80'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node80' AND B.name = 'node81'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node81' AND B.name = 'node82'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node82' AND B.name = 'node83'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node83' AND B.name = 'node84'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node84' AND B.name = 'node85'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node85' AND B.name = 'node86'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node86' AND B.name = 'node87'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node87' AND B.name = 'node88'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node88' AND B.name = 'node89'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node89' AND B.name = 'node90'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node90' AND B.name = 'node91'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node91' AND B.name = 'node92'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node92' AND B.name = 'node93'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node93' AND B.name = 'node94'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node94' AND B.name = 'node95'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node95' AND B.name = 'node96'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node96' AND B.name = 'node97'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node97' AND B.name = 'node98'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node98' AND B.name = 'node99'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node99' AND B.name = 'node100'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node100' AND B.name = 'node101'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node101' AND B.name = 'node102'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node102' AND B.name = 'node103'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node103' AND B.name = 'node104'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node104' AND B.name = 'node105'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node105' AND B.name = 'node106'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node106' AND B.name = 'node107'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node107' AND B.name = 'node108'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node108' AND B.name = 'node109'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node109' AND B.name = 'node110'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node110' AND B.name = 'node111'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node111' AND B.name = 'node112'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node112' AND B.name = 'node113'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node113' AND B.name = 'node114'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node114' AND B.name = 'node115'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node115' AND B.name = 'node116'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node116' AND B.name = 'node117'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node117' AND B.name = 'node118'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node118' AND B.name = 'node119'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node119' AND B.name = 'node120'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node120' AND B.name = 'node121'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node121' AND B.name = 'node122'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node122' AND B.name = 'node123'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node123' AND B.name = 'node124'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node124' AND B.name = 'node125'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node125' AND B.name = 'node126'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node126' AND B.name = 'node127'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node127' AND B.name = 'node128'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node128' AND B.name = 'node129'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node129' AND B.name = 'node130'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node130' AND B.name = 'node131'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node131' AND B.name = 'node132'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node132' AND B.name = 'node133'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node133' AND B.name = 'node134'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node134' AND B.name = 'node135'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node135' AND B.name = 'node136'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node136' AND B.name = 'node137'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node137' AND B.name = 'node138'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node138' AND B.name = 'node139'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node139' AND B.name = 'node140'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node140' AND B.name = 'node141'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node141' AND B.name = 'node142'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node142' AND B.name = 'node143'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node143' AND B.name = 'node144'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node144' AND B.name = 'node145'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node145' AND B.name = 'node146'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node146' AND B.name = 'node147'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node147' AND B.name = 'node148'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node148' AND B.name = 'node149'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node149' AND B.name = 'node150'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node150' AND B.name = 'node151'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node151' AND B.name = 'node152'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node152' AND B.name = 'node153'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node153' AND B.name = 'node154'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node154' AND B.name = 'node155'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node155' AND B.name = 'node156'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node156' AND B.name = 'node157'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node157' AND B.name = 'node158'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node158' AND B.name = 'node159'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node159' AND B.name = 'node160'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node160' AND B.name = 'node161'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node161' AND B.name = 'node162'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node162' AND B.name = 'node163'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node163' AND B.name = 'node164'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node164' AND B.name = 'node165'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node165' AND B.name = 'node166'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node166' AND B.name = 'node167'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node167' AND B.name = 'node168'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node168' AND B.name = 'node169'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node169' AND B.name = 'node170'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node170' AND B.name = 'node171'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node171' AND B.name = 'node172'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node172' AND B.name = 'node173'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node173' AND B.name = 'node174'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node174' AND B.name = 'node175'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node175' AND B.name = 'node176'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node176' AND B.name = 'node177'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node177' AND B.name = 'node178'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node178' AND B.name = 'node179'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node179' AND B.name = 'node180'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node180' AND B.name = 'node181'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node181' AND B.name = 'node182'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node182' AND B.name = 'node183'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node183' AND B.name = 'node184'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node184' AND B.name = 'node185'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node185' AND B.name = 'node186'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node186' AND B.name = 'node187'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node187' AND B.name = 'node188'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node188' AND B.name = 'node189'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node189' AND B.name = 'node190'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node190' AND B.name = 'node191'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node191' AND B.name = 'node192'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node192' AND B.name = 'node193'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node193' AND B.name = 'node194'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node194' AND B.name = 'node195'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node195' AND B.name = 'node196'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node196' AND B.name = 'node197'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node197' AND B.name = 'node198'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node198' AND B.name = 'node199'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node199' AND B.name = 'node200'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node200' AND B.name = 'node201'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node201' AND B.name = 'node202'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node202' AND B.name = 'node203'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node203' AND B.name = 'node204'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node204' AND B.name = 'node205'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node205' AND B.name = 'node206'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node206' AND B.name = 'node207'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node207' AND B.name = 'node208'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node208' AND B.name = 'node209'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node209' AND B.name = 'node210'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node210' AND B.name = 'node211'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node211' AND B.name = 'node212'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node212' AND B.name = 'node213'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node213' AND B.name = 'node214'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node214' AND B.name = 'node215'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node215' AND B.name = 'node216'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node216' AND B.name = 'node217'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node217' AND B.name = 'node218'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node218' AND B.name = 'node219'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node219' AND B.name = 'node220'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node220' AND B.name = 'node221'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node221' AND B.name = 'node222'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node222' AND B.name = 'node223'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node223' AND B.name = 'node224'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node224' AND B.name = 'node225'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node225' AND B.name = 'node226'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node226' AND B.name = 'node227'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node227' AND B.name = 'node228'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node228' AND B.name = 'node229'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node229' AND B.name = 'node230'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node230' AND B.name = 'node231'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node231' AND B.name = 'node232'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node232' AND B.name = 'node233'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node233' AND B.name = 'node234'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node234' AND B.name = 'node235'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node235' AND B.name = 'node236'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node236' AND B.name = 'node237'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node237' AND B.name = 'node238'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node238' AND B.name = 'node239'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node239' AND B.name = 'node240'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node240' AND B.name = 'node241'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node241' AND B.name = 'node242'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node242' AND B.name = 'node243'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node243' AND B.name = 'node244'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node244' AND B.name = 'node245'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node245' AND B.name = 'node246'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node246' AND B.name = 'node247'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node247' AND B.name = 'node248'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node248' AND B.name = 'node249'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node249' AND B.name = 'node250'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node250' AND B.name = 'node251'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node251' AND B.name = 'node252'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node252' AND B.name = 'node253'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node253' AND B.name = 'node254'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node254' AND B.name = 'node255'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node255' AND B.name = 'node256'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node256' AND B.name = 'node257'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node257' AND B.name = 'node258'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node258' AND B.name = 'node259'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node259' AND B.name = 'node260'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node260' AND B.name = 'node261'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node261' AND B.name = 'node262'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node262' AND B.name = 'node263'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node263' AND B.name = 'node264'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node264' AND B.name = 'node265'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node265' AND B.name = 'node266'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node266' AND B.name = 'node267'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node267' AND B.name = 'node268'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node268' AND B.name = 'node269'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node269' AND B.name = 'node270'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node270' AND B.name = 'node271'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node271' AND B.name = 'node272'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node272' AND B.name = 'node273'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node273' AND B.name = 'node274'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node274' AND B.name = 'node275'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node275' AND B.name = 'node276'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node276' AND B.name = 'node277'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node277' AND B.name = 'node278'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node278' AND B.name = 'node279'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node279' AND B.name = 'node280'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node280' AND B.name = 'node281'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node281' AND B.name = 'node282'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node282' AND B.name = 'node283'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node283' AND B.name = 'node284'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node284' AND B.name = 'node285'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node285' AND B.name = 'node286'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node286' AND B.name = 'node287'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node287' AND B.name = 'node288'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node288' AND B.name = 'node289'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node289' AND B.name = 'node290'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node290' AND B.name = 'node291'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node291' AND B.name = 'node292'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node292' AND B.name = 'node293'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node293' AND B.name = 'node294'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node294' AND B.name = 'node295'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node295' AND B.name = 'node296'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node296' AND B.name = 'node297'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node297' AND B.name = 'node298'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node298' AND B.name = 'node299'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node299' AND B.name = 'node300'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node300' AND B.name = 'node301'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node301' AND B.name = 'node302'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node302' AND B.name = 'node303'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node303' AND B.name = 'node304'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node304' AND B.name = 'node305'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node305' AND B.name = 'node306'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node306' AND B.name = 'node307'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node307' AND B.name = 'node308'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node308' AND B.name = 'node309'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node309' AND B.name = 'node310'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node310' AND B.name = 'node311'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node311' AND B.name = 'node312'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node312' AND B.name = 'node313'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node313' AND B.name = 'node314'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node314' AND B.name = 'node315'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node315' AND B.name = 'node316'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node316' AND B.name = 'node317'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node317' AND B.name = 'node318'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node318' AND B.name = 'node319'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node319' AND B.name = 'node320'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node320' AND B.name = 'node321'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node321' AND B.name = 'node322'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node322' AND B.name = 'node323'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node323' AND B.name = 'node324'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node324' AND B.name = 'node325'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node325' AND B.name = 'node326'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node326' AND B.name = 'node327'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node327' AND B.name = 'node328'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node328' AND B.name = 'node329'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node329' AND B.name = 'node330'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node330' AND B.name = 'node331'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node331' AND B.name = 'node332'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node332' AND B.name = 'node333'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node333' AND B.name = 'node334'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node334' AND B.name = 'node335'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node335' AND B.name = 'node336'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node336' AND B.name = 'node337'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node337' AND B.name = 'node338'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node338' AND B.name = 'node339'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node339' AND B.name = 'node340'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node340' AND B.name = 'node341'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node341' AND B.name = 'node342'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node342' AND B.name = 'node343'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node343' AND B.name = 'node344'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node344' AND B.name = 'node345'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node345' AND B.name = 'node346'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node346' AND B.name = 'node347'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node347' AND B.name = 'node348'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node348' AND B.name = 'node349'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node349' AND B.name = 'node350'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node350' AND B.name = 'node351'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node351' AND B.name = 'node352'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node352' AND B.name = 'node353'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node353' AND B.name = 'node354'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node354' AND B.name = 'node355'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node355' AND B.name = 'node356'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node356' AND B.name = 'node357'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node357' AND B.name = 'node358'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node358' AND B.name = 'node359'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node359' AND B.name = 'node360'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node360' AND B.name = 'node361'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node361' AND B.name = 'node362'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node362' AND B.name = 'node363'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node363' AND B.name = 'node364'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node364' AND B.name = 'node365'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node365' AND B.name = 'node366'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node366' AND B.name = 'node367'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node367' AND B.name = 'node368'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node368' AND B.name = 'node369'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node369' AND B.name = 'node370'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node370' AND B.name = 'node371'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node371' AND B.name = 'node372'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node372' AND B.name = 'node373'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node373' AND B.name = 'node374'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node374' AND B.name = 'node375'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node375' AND B.name = 'node376'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node376' AND B.name = 'node377'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node377' AND B.name = 'node378'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node378' AND B.name = 'node379'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node379' AND B.name = 'node380'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node380' AND B.name = 'node381'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node381' AND B.name = 'node382'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node382' AND B.name = 'node383'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node383' AND B.name = 'node384'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node384' AND B.name = 'node385'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node385' AND B.name = 'node386'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node386' AND B.name = 'node387'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node387' AND B.name = 'node388'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node388' AND B.name = 'node389'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node389' AND B.name = 'node390'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node390' AND B.name = 'node391'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node391' AND B.name = 'node392'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node392' AND B.name = 'node393'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node393' AND B.name = 'node394'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node394' AND B.name = 'node395'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node395' AND B.name = 'node396'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node396' AND B.name = 'node397'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node397' AND B.name = 'node398'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node398' AND B.name = 'node399'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node399' AND B.name = 'node400'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node400' AND B.name = 'node401'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node401' AND B.name = 'node402'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node402' AND B.name = 'node403'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node403' AND B.name = 'node404'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node404' AND B.name = 'node405'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node405' AND B.name = 'node406'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node406' AND B.name = 'node407'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node407' AND B.name = 'node408'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node408' AND B.name = 'node409'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node409' AND B.name = 'node410'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node410' AND B.name = 'node411'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node411' AND B.name = 'node412'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node412' AND B.name = 'node413'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node413' AND B.name = 'node414'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node414' AND B.name = 'node415'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node415' AND B.name = 'node416'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node416' AND B.name = 'node417'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node417' AND B.name = 'node418'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node418' AND B.name = 'node419'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node419' AND B.name = 'node420'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node420' AND B.name = 'node421'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node421' AND B.name = 'node422'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node422' AND B.name = 'node423'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node423' AND B.name = 'node424'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node424' AND B.name = 'node425'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node425' AND B.name = 'node426'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node426' AND B.name = 'node427'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node427' AND B.name = 'node428'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node428' AND B.name = 'node429'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node429' AND B.name = 'node430'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node430' AND B.name = 'node431'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node431' AND B.name = 'node432'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node432' AND B.name = 'node433'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node433' AND B.name = 'node434'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node434' AND B.name = 'node435'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node435' AND B.name = 'node436'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node436' AND B.name = 'node437'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node437' AND B.name = 'node438'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node438' AND B.name = 'node439'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node439' AND B.name = 'node440'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node440' AND B.name = 'node441'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node441' AND B.name = 'node442'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node442' AND B.name = 'node443'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node443' AND B.name = 'node444'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node444' AND B.name = 'node445'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node445' AND B.name = 'node446'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node446' AND B.name = 'node447'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node447' AND B.name = 'node448'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node448' AND B.name = 'node449'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node449' AND B.name = 'node450'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node450' AND B.name = 'node451'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node451' AND B.name = 'node452'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node452' AND B.name = 'node453'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node453' AND B.name = 'node454'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node454' AND B.name = 'node455'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node455' AND B.name = 'node456'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node456' AND B.name = 'node457'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node457' AND B.name = 'node458'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node458' AND B.name = 'node459'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node459' AND B.name = 'node460'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node460' AND B.name = 'node461'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node461' AND B.name = 'node462'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node462' AND B.name = 'node463'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node463' AND B.name = 'node464'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node464' AND B.name = 'node465'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node465' AND B.name = 'node466'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node466' AND B.name = 'node467'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node467' AND B.name = 'node468'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node468' AND B.name = 'node469'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node469' AND B.name = 'node470'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node470' AND B.name = 'node471'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node471' AND B.name = 'node472'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node472' AND B.name = 'node473'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node473' AND B.name = 'node474'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node474' AND B.name = 'node475'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node475' AND B.name = 'node476'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node476' AND B.name = 'node477'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node477' AND B.name = 'node478'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node478' AND B.name = 'node479'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node479' AND B.name = 'node480'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node480' AND B.name = 'node481'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node481' AND B.name = 'node482'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node482' AND B.name = 'node483'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node483' AND B.name = 'node484'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node484' AND B.name = 'node485'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node485' AND B.name = 'node486'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node486' AND B.name = 'node487'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node487' AND B.name = 'node488'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node488' AND B.name = 'node489'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node489' AND B.name = 'node490'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node490' AND B.name = 'node491'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node491' AND B.name = 'node492'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node492' AND B.name = 'node493'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node493' AND B.name = 'node494'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node494' AND B.name = 'node495'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node495' AND B.name = 'node496'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node496' AND B.name = 'node497'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node497' AND B.name = 'node498'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node498' AND B.name = 'node499'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node499' AND B.name = 'node500'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node500' AND B.name = 'node501'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node501' AND B.name = 'node502'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node502' AND B.name = 'node503'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node503' AND B.name = 'node504'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node504' AND B.name = 'node505'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node505' AND B.name = 'node506'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node506' AND B.name = 'node507'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node507' AND B.name = 'node508'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node508' AND B.name = 'node509'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node509' AND B.name = 'node510'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node510' AND B.name = 'node511'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node511' AND B.name = 'node512'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node512' AND B.name = 'node513'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node513' AND B.name = 'node514'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node514' AND B.name = 'node515'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node515' AND B.name = 'node516'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node516' AND B.name = 'node517'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node517' AND B.name = 'node518'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node518' AND B.name = 'node519'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node519' AND B.name = 'node520'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node520' AND B.name = 'node521'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node521' AND B.name = 'node522'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node522' AND B.name = 'node523'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node523' AND B.name = 'node524'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node524' AND B.name = 'node525'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node525' AND B.name = 'node526'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node526' AND B.name = 'node527'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node527' AND B.name = 'node528'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node528' AND B.name = 'node529'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node529' AND B.name = 'node530'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node530' AND B.name = 'node531'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node531' AND B.name = 'node532'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node532' AND B.name = 'node533'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node533' AND B.name = 'node534'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node534' AND B.name = 'node535'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node535' AND B.name = 'node536'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node536' AND B.name = 'node537'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node537' AND B.name = 'node538'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node538' AND B.name = 'node539'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node539' AND B.name = 'node540'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node540' AND B.name = 'node541'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node541' AND B.name = 'node542'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node542' AND B.name = 'node543'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node543' AND B.name = 'node544'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node544' AND B.name = 'node545'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node545' AND B.name = 'node546'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node546' AND B.name = 'node547'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node547' AND B.name = 'node548'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node548' AND B.name = 'node549'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node549' AND B.name = 'node550'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node550' AND B.name = 'node551'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node551' AND B.name = 'node552'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node552' AND B.name = 'node553'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node553' AND B.name = 'node554'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node554' AND B.name = 'node555'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node555' AND B.name = 'node556'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node556' AND B.name = 'node557'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node557' AND B.name = 'node558'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node558' AND B.name = 'node559'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node559' AND B.name = 'node560'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node560' AND B.name = 'node561'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node561' AND B.name = 'node562'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node562' AND B.name = 'node563'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node563' AND B.name = 'node564'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node564' AND B.name = 'node565'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node565' AND B.name = 'node566'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node566' AND B.name = 'node567'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node567' AND B.name = 'node568'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node568' AND B.name = 'node569'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node569' AND B.name = 'node570'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node570' AND B.name = 'node571'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node571' AND B.name = 'node572'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node572' AND B.name = 'node573'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node573' AND B.name = 'node574'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node574' AND B.name = 'node575'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node575' AND B.name = 'node576'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node576' AND B.name = 'node577'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node577' AND B.name = 'node578'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node578' AND B.name = 'node579'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node579' AND B.name = 'node580'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node580' AND B.name = 'node581'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node581' AND B.name = 'node582'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node582' AND B.name = 'node583'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node583' AND B.name = 'node584'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node584' AND B.name = 'node585'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node585' AND B.name = 'node586'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node586' AND B.name = 'node587'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node587' AND B.name = 'node588'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node588' AND B.name = 'node589'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node589' AND B.name = 'node590'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node590' AND B.name = 'node591'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node591' AND B.name = 'node592'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node592' AND B.name = 'node593'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node593' AND B.name = 'node594'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node594' AND B.name = 'node595'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node595' AND B.name = 'node596'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node596' AND B.name = 'node597'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node597' AND B.name = 'node598'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node598' AND B.name = 'node599'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node599' AND B.name = 'node600'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node600' AND B.name = 'node601'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node601' AND B.name = 'node602'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node602' AND B.name = 'node603'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node603' AND B.name = 'node604'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node604' AND B.name = 'node605'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node605' AND B.name = 'node606'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node606' AND B.name = 'node607'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node607' AND B.name = 'node608'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node608' AND B.name = 'node609'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node609' AND B.name = 'node610'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node610' AND B.name = 'node611'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node611' AND B.name = 'node612'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node612' AND B.name = 'node613'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node613' AND B.name = 'node614'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node614' AND B.name = 'node615'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node615' AND B.name = 'node616'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node616' AND B.name = 'node617'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node617' AND B.name = 'node618'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node618' AND B.name = 'node619'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node619' AND B.name = 'node620'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node620' AND B.name = 'node621'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node621' AND B.name = 'node622'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node622' AND B.name = 'node623'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node623' AND B.name = 'node624'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node624' AND B.name = 'node625'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node625' AND B.name = 'node626'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node626' AND B.name = 'node627'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node627' AND B.name = 'node628'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node628' AND B.name = 'node629'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node629' AND B.name = 'node630'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node630' AND B.name = 'node631'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node631' AND B.name = 'node632'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node632' AND B.name = 'node633'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node633' AND B.name = 'node634'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node634' AND B.name = 'node635'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node635' AND B.name = 'node636'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node636' AND B.name = 'node637'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node637' AND B.name = 'node638'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node638' AND B.name = 'node639'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node639' AND B.name = 'node640'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node640' AND B.name = 'node641'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node641' AND B.name = 'node642'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node642' AND B.name = 'node643'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node643' AND B.name = 'node644'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node644' AND B.name = 'node645'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node645' AND B.name = 'node646'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node646' AND B.name = 'node647'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node647' AND B.name = 'node648'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node648' AND B.name = 'node649'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node649' AND B.name = 'node650'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node650' AND B.name = 'node651'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node651' AND B.name = 'node652'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node652' AND B.name = 'node653'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node653' AND B.name = 'node654'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node654' AND B.name = 'node655'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node655' AND B.name = 'node656'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node656' AND B.name = 'node657'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node657' AND B.name = 'node658'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node658' AND B.name = 'node659'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node659' AND B.name = 'node660'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node660' AND B.name = 'node661'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node661' AND B.name = 'node662'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node662' AND B.name = 'node663'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node663' AND B.name = 'node664'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node664' AND B.name = 'node665'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node665' AND B.name = 'node666'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node666' AND B.name = 'node667'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node667' AND B.name = 'node668'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node668' AND B.name = 'node669'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node669' AND B.name = 'node670'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node670' AND B.name = 'node671'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node671' AND B.name = 'node672'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node672' AND B.name = 'node673'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node673' AND B.name = 'node674'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node674' AND B.name = 'node675'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node675' AND B.name = 'node676'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node676' AND B.name = 'node677'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node677' AND B.name = 'node678'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node678' AND B.name = 'node679'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node679' AND B.name = 'node680'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node680' AND B.name = 'node681'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node681' AND B.name = 'node682'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node682' AND B.name = 'node683'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node683' AND B.name = 'node684'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node684' AND B.name = 'node685'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node685' AND B.name = 'node686'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node686' AND B.name = 'node687'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node687' AND B.name = 'node688'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node688' AND B.name = 'node689'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node689' AND B.name = 'node690'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node690' AND B.name = 'node691'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node691' AND B.name = 'node692'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node692' AND B.name = 'node693'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node693' AND B.name = 'node694'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node694' AND B.name = 'node695'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node695' AND B.name = 'node696'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node696' AND B.name = 'node697'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node697' AND B.name = 'node698'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node698' AND B.name = 'node699'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node699' AND B.name = 'node700'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node700' AND B.name = 'node701'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node701' AND B.name = 'node702'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node702' AND B.name = 'node703'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node703' AND B.name = 'node704'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node704' AND B.name = 'node705'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node705' AND B.name = 'node706'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node706' AND B.name = 'node707'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node707' AND B.name = 'node708'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node708' AND B.name = 'node709'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node709' AND B.name = 'node710'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node710' AND B.name = 'node711'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node711' AND B.name = 'node712'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node712' AND B.name = 'node713'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node713' AND B.name = 'node714'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node714' AND B.name = 'node715'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node715' AND B.name = 'node716'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node716' AND B.name = 'node717'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node717' AND B.name = 'node718'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node718' AND B.name = 'node719'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node719' AND B.name = 'node720'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node720' AND B.name = 'node721'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node721' AND B.name = 'node722'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node722' AND B.name = 'node723'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node723' AND B.name = 'node724'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node724' AND B.name = 'node725'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node725' AND B.name = 'node726'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node726' AND B.name = 'node727'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node727' AND B.name = 'node728'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node728' AND B.name = 'node729'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node729' AND B.name = 'node730'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node730' AND B.name = 'node731'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node731' AND B.name = 'node732'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node732' AND B.name = 'node733'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node733' AND B.name = 'node734'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node734' AND B.name = 'node735'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node735' AND B.name = 'node736'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node736' AND B.name = 'node737'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node737' AND B.name = 'node738'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node738' AND B.name = 'node739'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node739' AND B.name = 'node740'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node740' AND B.name = 'node741'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node741' AND B.name = 'node742'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node742' AND B.name = 'node743'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node743' AND B.name = 'node744'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node744' AND B.name = 'node745'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node745' AND B.name = 'node746'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node746' AND B.name = 'node747'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node747' AND B.name = 'node748'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node748' AND B.name = 'node749'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node749' AND B.name = 'node750'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node750' AND B.name = 'node751'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node751' AND B.name = 'node752'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node752' AND B.name = 'node753'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node753' AND B.name = 'node754'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node754' AND B.name = 'node755'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node755' AND B.name = 'node756'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node756' AND B.name = 'node757'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node757' AND B.name = 'node758'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node758' AND B.name = 'node759'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node759' AND B.name = 'node760'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node760' AND B.name = 'node761'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node761' AND B.name = 'node762'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node762' AND B.name = 'node763'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node763' AND B.name = 'node764'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node764' AND B.name = 'node765'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node765' AND B.name = 'node766'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node766' AND B.name = 'node767'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node767' AND B.name = 'node768'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node768' AND B.name = 'node769'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node769' AND B.name = 'node770'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node770' AND B.name = 'node771'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node771' AND B.name = 'node772'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node772' AND B.name = 'node773'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node773' AND B.name = 'node774'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node774' AND B.name = 'node775'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node775' AND B.name = 'node776'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node776' AND B.name = 'node777'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node777' AND B.name = 'node778'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node778' AND B.name = 'node779'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node779' AND B.name = 'node780'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node780' AND B.name = 'node781'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node781' AND B.name = 'node782'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node782' AND B.name = 'node783'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node783' AND B.name = 'node784'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node784' AND B.name = 'node785'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node785' AND B.name = 'node786'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node786' AND B.name = 'node787'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node787' AND B.name = 'node788'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node788' AND B.name = 'node789'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node789' AND B.name = 'node790'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node790' AND B.name = 'node791'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node791' AND B.name = 'node792'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node792' AND B.name = 'node793'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node793' AND B.name = 'node794'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node794' AND B.name = 'node795'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node795' AND B.name = 'node796'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node796' AND B.name = 'node797'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node797' AND B.name = 'node798'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node798' AND B.name = 'node799'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node799' AND B.name = 'node800'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node800' AND B.name = 'node801'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node801' AND B.name = 'node802'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node802' AND B.name = 'node803'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node803' AND B.name = 'node804'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node804' AND B.name = 'node805'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node805' AND B.name = 'node806'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node806' AND B.name = 'node807'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node807' AND B.name = 'node808'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node808' AND B.name = 'node809'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node809' AND B.name = 'node810'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node810' AND B.name = 'node811'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node811' AND B.name = 'node812'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node812' AND B.name = 'node813'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node813' AND B.name = 'node814'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node814' AND B.name = 'node815'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node815' AND B.name = 'node816'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node816' AND B.name = 'node817'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node817' AND B.name = 'node818'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node818' AND B.name = 'node819'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node819' AND B.name = 'node820'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node820' AND B.name = 'node821'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node821' AND B.name = 'node822'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node822' AND B.name = 'node823'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node823' AND B.name = 'node824'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node824' AND B.name = 'node825'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node825' AND B.name = 'node826'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node826' AND B.name = 'node827'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node827' AND B.name = 'node828'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node828' AND B.name = 'node829'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node829' AND B.name = 'node830'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node830' AND B.name = 'node831'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node831' AND B.name = 'node832'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node832' AND B.name = 'node833'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node833' AND B.name = 'node834'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node834' AND B.name = 'node835'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node835' AND B.name = 'node836'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node836' AND B.name = 'node837'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node837' AND B.name = 'node838'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node838' AND B.name = 'node839'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node839' AND B.name = 'node840'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node840' AND B.name = 'node841'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node841' AND B.name = 'node842'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node842' AND B.name = 'node843'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node843' AND B.name = 'node844'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node844' AND B.name = 'node845'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node845' AND B.name = 'node846'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node846' AND B.name = 'node847'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node847' AND B.name = 'node848'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node848' AND B.name = 'node849'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node849' AND B.name = 'node850'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node850' AND B.name = 'node851'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node851' AND B.name = 'node852'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node852' AND B.name = 'node853'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node853' AND B.name = 'node854'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node854' AND B.name = 'node855'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node855' AND B.name = 'node856'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node856' AND B.name = 'node857'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node857' AND B.name = 'node858'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node858' AND B.name = 'node859'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node859' AND B.name = 'node860'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node860' AND B.name = 'node861'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node861' AND B.name = 'node862'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node862' AND B.name = 'node863'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node863' AND B.name = 'node864'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node864' AND B.name = 'node865'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node865' AND B.name = 'node866'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node866' AND B.name = 'node867'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node867' AND B.name = 'node868'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node868' AND B.name = 'node869'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node869' AND B.name = 'node870'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node870' AND B.name = 'node871'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node871' AND B.name = 'node872'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node872' AND B.name = 'node873'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node873' AND B.name = 'node874'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node874' AND B.name = 'node875'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node875' AND B.name = 'node876'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node876' AND B.name = 'node877'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node877' AND B.name = 'node878'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node878' AND B.name = 'node879'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node879' AND B.name = 'node880'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node880' AND B.name = 'node881'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node881' AND B.name = 'node882'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node882' AND B.name = 'node883'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node883' AND B.name = 'node884'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node884' AND B.name = 'node885'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node885' AND B.name = 'node886'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node886' AND B.name = 'node887'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node887' AND B.name = 'node888'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node888' AND B.name = 'node889'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node889' AND B.name = 'node890'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node890' AND B.name = 'node891'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node891' AND B.name = 'node892'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node892' AND B.name = 'node893'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node893' AND B.name = 'node894'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node894' AND B.name = 'node895'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node895' AND B.name = 'node896'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node896' AND B.name = 'node897'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node897' AND B.name = 'node898'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node898' AND B.name = 'node899'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node899' AND B.name = 'node900'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node900' AND B.name = 'node901'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node901' AND B.name = 'node902'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node902' AND B.name = 'node903'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node903' AND B.name = 'node904'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node904' AND B.name = 'node905'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node905' AND B.name = 'node906'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node906' AND B.name = 'node907'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node907' AND B.name = 'node908'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node908' AND B.name = 'node909'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node909' AND B.name = 'node910'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node910' AND B.name = 'node911'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node911' AND B.name = 'node912'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node912' AND B.name = 'node913'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node913' AND B.name = 'node914'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node914' AND B.name = 'node915'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node915' AND B.name = 'node916'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node916' AND B.name = 'node917'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node917' AND B.name = 'node918'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node918' AND B.name = 'node919'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node919' AND B.name = 'node920'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node920' AND B.name = 'node921'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node921' AND B.name = 'node922'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node922' AND B.name = 'node923'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node923' AND B.name = 'node924'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node924' AND B.name = 'node925'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node925' AND B.name = 'node926'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node926' AND B.name = 'node927'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node927' AND B.name = 'node928'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node928' AND B.name = 'node929'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node929' AND B.name = 'node930'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node930' AND B.name = 'node931'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node931' AND B.name = 'node932'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node932' AND B.name = 'node933'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node933' AND B.name = 'node934'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node934' AND B.name = 'node935'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node935' AND B.name = 'node936'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node936' AND B.name = 'node937'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node937' AND B.name = 'node938'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node938' AND B.name = 'node939'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node939' AND B.name = 'node940'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node940' AND B.name = 'node941'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node941' AND B.name = 'node942'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node942' AND B.name = 'node943'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node943' AND B.name = 'node944'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node944' AND B.name = 'node945'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node945' AND B.name = 'node946'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node946' AND B.name = 'node947'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node947' AND B.name = 'node948'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node948' AND B.name = 'node949'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node949' AND B.name = 'node950'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node950' AND B.name = 'node951'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node951' AND B.name = 'node952'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node952' AND B.name = 'node953'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node953' AND B.name = 'node954'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node954' AND B.name = 'node955'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node955' AND B.name = 'node956'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node956' AND B.name = 'node957'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node957' AND B.name = 'node958'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node958' AND B.name = 'node959'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node959' AND B.name = 'node960'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node960' AND B.name = 'node961'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node961' AND B.name = 'node962'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node962' AND B.name = 'node963'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node963' AND B.name = 'node964'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node964' AND B.name = 'node965'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node965' AND B.name = 'node966'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node966' AND B.name = 'node967'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node967' AND B.name = 'node968'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node968' AND B.name = 'node969'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node969' AND B.name = 'node970'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node970' AND B.name = 'node971'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node971' AND B.name = 'node972'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node972' AND B.name = 'node973'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node973' AND B.name = 'node974'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node974' AND B.name = 'node975'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node975' AND B.name = 'node976'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node976' AND B.name = 'node977'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node977' AND B.name = 'node978'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node978' AND B.name = 'node979'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node979' AND B.name = 'node980'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node980' AND B.name = 'node981'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node981' AND B.name = 'node982'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node982' AND B.name = 'node983'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node983' AND B.name = 'node984'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node984' AND B.name = 'node985'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node985' AND B.name = 'node986'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node986' AND B.name = 'node987'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node987' AND B.name = 'node988'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node988' AND B.name = 'node989'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node989' AND B.name = 'node990'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node990' AND B.name = 'node991'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node991' AND B.name = 'node992'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node992' AND B.name = 'node993'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node993' AND B.name = 'node994'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node994' AND B.name = 'node995'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node995' AND B.name = 'node996'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node996' AND B.name = 'node997'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node997' AND B.name = 'node998'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node998' AND B.name = 'node999'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node999' AND B.name = 'node1000'


INSERT INTO Edge (type)
SELECT A, B, 'links'
FROM   Node A, Node B
WHERE  A.name = 'node1000' AND B.name = 'node1'


            ";
#endregion
            gcmd.ExecuteNonQuery();
        }
    }
}
