using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace ConsoleApplication1
{
    internal class Program
    {

        public static void Inicialize()
        {
            using (SqlConnection connection =
                new SqlConnection(
                    @"Data Source=SOFTENGMULTI\SQL2008;Initial Catalog=ProjectTemplate_Dev;Integrated Security=True;Connect Timeout=15;Encrypt=False;TrustServerCertificate=False")
                )
            {
                connection.Open();

                SQLUpdateValue(connection,"1");
                connection.Close();
                PrintMessage("Initial Value=1",5);
            }
        }

        public static void FinalValue()
        {
            using (var connection =
                new SqlConnection(
                    @"Data Source=SOFTENGMULTI\SQL2008;Initial Catalog=ProjectTemplate_Dev;Integrated Security=True;Connect Timeout=15;Encrypt=False;TrustServerCertificate=False")
                )
            {

                connection.Open();
                var value = SQLReturnValue(connection);
                PrintMessage("Final Value=" + value,5);
                connection.Close();
            }
        }


        private static void Main(string[] args)
        {
            var options = new TransactionOptions
                {
                    IsolationLevel = IsolationLevel.ReadCommitted,
                    Timeout = new TimeSpan(0, 0, 0, 10)
                };


            PrintMessage("ENTITY ReadCommitted - ",0);
            LanzaTheads(JobEntity(options), options);

            PrintMessage("SQL ReadCommitted", 0);
            LanzaTheads(JobSQL(options), options);

            PrintMessage("SQLUpdLock ReadCommitted", 0);
            LanzaTheads(JobSQLUpdLock(options), options);

            options = new TransactionOptions
            {
                IsolationLevel = IsolationLevel.RepeatableRead,
                Timeout = new TimeSpan(0, 0, 0, 10)
            };

            PrintMessage("ENTITY RepeatableRead - ", 0);
            LanzaTheads(JobEntity(options), options);

            PrintMessage("SQL RepeatableRead", 0);
            LanzaTheads(JobSQL(options), options);

            PrintMessage("SQLUpdLock RepeatableRead", 0);
            LanzaTheads(JobSQLUpdLock(options), options);
            
            Console.ReadKey();
        }

        private static void LanzaTheads(WaitCallback job, TransactionOptions options)
        {
            Inicialize();


            var handles = new WaitHandle[]
                {
                    new ManualResetEvent(false),
                    new ManualResetEvent(false),
                };
            // Run workers
            ThreadPool.QueueUserWorkItem(job, handles[0]);
            ThreadPool.QueueUserWorkItem(job, handles[1]);

            // Wait for both workers to complete
            WaitHandle.WaitAll(handles);

            FinalValue();
        }

        private static void PrintMessage(String message, int level = 10)
        {
            var s = new String(' ', level);
            s = s + System.Threading.Thread.CurrentThread.ManagedThreadId.ToString() + ":";
            s = s + message;
            Console.WriteLine(s);
        }


        private static WaitCallback JobEntity(TransactionOptions options)
        {
            return state =>
                {
                    try
                    {
                        using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew, options))
                        {
                            using (var context = new ProjectTemplate_DevEntities())
                            {
                                var element = context.Element.First(d => d.Id == "d0530ea7-639f-4d5e-91fb-1a4fed1debfe");
                                var intInternalName = Int32.Parse(element.InternalName);
                                PrintMessage("ReadValue=" + intInternalName.ToString());
                                element.InternalName = (intInternalName + 1).ToString();
                                PrintMessage("Incrementing to" + element.InternalName);
                                Thread.Sleep(2000);
                                context.SaveChanges(); // Save changes to DB
                                PrintMessage("Saving");
                                Thread.Sleep(2000);
                            }
                            scope.Complete(); // Commit transaction
                            PrintMessage("Commit");
                        }
                    }
                    catch (Exception e)
                    {
                        PrintMessage( e.Message);
                    }finally
                    {
                        ((ManualResetEvent)state).Set();
                    }
                };
        }

        private static WaitCallback JobSQL(TransactionOptions options)
        {
            return state =>
                {
                    try
                    {
                        using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew, options))
                        {

                            using (var connection =
                                new SqlConnection(
                                    @"Data Source=SOFTENGMULTI\SQL2008;Initial Catalog=ProjectTemplate_Dev;Integrated Security=True;Connect Timeout=15;Encrypt=False;TrustServerCertificate=False")
                                )
                            {
                                connection.Open();
                                var value = SQLReturnValue(connection);
                                var intInternalName = Int32.Parse(value.ToString());
                                PrintMessage("ReadValue=" + intInternalName.ToString());
                                var newvalue = (intInternalName + 1).ToString();
                                PrintMessage("Incrementing to" + newvalue);
                                Thread.Sleep(2000);
                                SQLUpdateValue(connection, newvalue);
                                PrintMessage("Saving");
                                Thread.Sleep(2000);
                                connection.Close();
                            }
                            scope.Complete(); // Commit transaction
                            PrintMessage("Commit");
                        }
                    }
                    catch (Exception e)
                    {
                        PrintMessage(e.Message);
                    }
                    finally
                    {
                        ((ManualResetEvent) state).Set();
                    }
                };
        }

        private static WaitCallback JobSQLUpdLock(TransactionOptions options)
        {
            return state =>
            {
                try
                {
                    using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew, options))
                    {

                        using (var connection =
                            new SqlConnection(
                                @"Data Source=SOFTENGMULTI\SQL2008;Initial Catalog=ProjectTemplate_Dev;Integrated Security=True;Connect Timeout=15;Encrypt=False;TrustServerCertificate=False")
                            )
                        {
                            connection.Open();
                            var value = SQLReturnUpdLockValue(connection);
                            var intInternalName = Int32.Parse(value.ToString());
                            PrintMessage("ReadValue=" + intInternalName.ToString());
                            var newvalue = (intInternalName + 1).ToString();
                            PrintMessage("Incrementing to" + newvalue);
                            Thread.Sleep(2000);
                            SQLUpdateValue(connection, newvalue);
                            PrintMessage("Saving");
                            Thread.Sleep(2000);
                            connection.Close();
                        }
                        scope.Complete(); // Commit transaction
                        PrintMessage("Commit");
                    }
                }
                catch (Exception e)
                {
                    PrintMessage(e.Message);
                }
                finally
                {
                    ((ManualResetEvent)state).Set();
                }
            };
        }

        #region SQL

        private static object SQLReturnUpdLockValue(SqlConnection connection)
        {
            var sql = "select InternalName from element with (updlock) where ID = 'd0530ea7-639f-4d5e-91fb-1a4fed1debfe'";
            // Create the Command and Parameter objects.
            var command = new SqlCommand(sql, connection);
            // Open the connection in a try/catch block. 
            // Create and execute the DataReader, writing the result
            // set to the console window.                            
            var intInternalName = command.ExecuteScalar();
            return intInternalName;
        }


        private static object SQLReturnValue(SqlConnection connection)
        {
            var sql = "select InternalName from element where ID = 'd0530ea7-639f-4d5e-91fb-1a4fed1debfe'";
            // Create the Command and Parameter objects.
            var command = new SqlCommand(sql, connection);
            // Open the connection in a try/catch block. 
            // Create and execute the DataReader, writing the result
            // set to the console window.                            
            var intInternalName = command.ExecuteScalar();
            return intInternalName;
        }
        private static void SQLUpdateValue(SqlConnection connection, string value)
        {
            var sql = "update element set InternalName = '{0}' where ID = 'd0530ea7-639f-4d5e-91fb-1a4fed1debfe'";
            var sqlexec = String.Format(sql, value);

            // Create the Command and Parameter objects.
            var command = new SqlCommand(sqlexec, connection);

            // Open the connection in a try/catch block. 
            // Create and execute the DataReader, writing the result
            // set to the console window.
            command.ExecuteNonQuery();
        }
        #endregion
    }
}
