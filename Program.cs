using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GetDataIntoMySQL
{
    /// <summary>
    /// Data Detectives - Ted Sill, Kyle Brennan, and Sawan-Kumar Jindal
    /// Introduction to Big Data Fall 2015
    /// Console application which takes a dataset in csv format and 
    /// feeds it into a relational schema.
    /// </summary>
    class Program
    {
        //Store the 'root' database password
        private static string password = String.Empty;

        /// <summary>
        /// The main entry point of the program.
        /// </summary>
        /// <param name="args">Not used</param>
        static void Main(string[] args)
        {
            //Prompt the user for the root database password
            PromptUserForPassword();
            //Populate the tables which have static data
            PopulateLookupTables();
            //Populate the tables which are dependant on the input file
            PopulateValuesFromCSV();
            //Notify the user we have finished
            Console.WriteLine("Finished Populating Database");
            //Wait for 'Enter' key
            Console.ReadLine();
        }

        /// <summary>
        /// Prompt the user for their 'root' password
        /// </summary>
        private static void PromptUserForPassword()
        {
            do
            {
                Console.Write("Please enter your 'root' database password: ");
                password = Console.ReadLine().Trim();
            } while (password.Equals(String.Empty));
        }

        /// <summary>
        /// Populate tables which contain static data.  This was 
        /// designed to be idempotent, checking for the existance
        /// or records before adding them anew.
        /// </summary>
        private static void PopulateLookupTables()
        {
            //Instantiate a sql repository through which we will interact with the database
            MySQLRepository myRepo = new MySQLRepository(password);
            //If we are able to open the connection
            if (myRepo.OpenConnection() == true)
            {
                //Populate the table containing the days of the week
                myRepo.CreateDays();
                //Populate the categories table
                myRepo.CreateCategories();
                //Populate the ranks table
                myRepo.CreateRanks();
                //Populate the classifications table
                myRepo.CreateClassifications();
                //Close the connection
                myRepo.CloseConnection();
            }
            else
            {
                Console.WriteLine("Connection not open");
            }
        }

        /// <summary>
        /// Read the csv and populate the database
        /// </summary>
        private static void PopulateValuesFromCSV()
        {
            //The path of the csv file
            string filePath = AppDomain.CurrentDomain.BaseDirectory + @"OnlineNewsPopularity.csv";
            //Read the file line by line
            var reader = new StreamReader(File.OpenRead(filePath));
            //Discard the line containing the headers
            if (!reader.EndOfStream){ reader.ReadLine(); }

            //Create a MySQL repository to interact with the database
            MySQLRepository myRepo = new MySQLRepository(password);
            int count = 0;
            //Iterate over each line in the file inserting values into the database
            while (!reader.EndOfStream)
            {
                //Read one line
                var line = reader.ReadLine();
                //Split the attributes which are delimited by a comma
                var values = line.Split(',');

                //Add the article record
                int article_id = (int)myRepo.AddArticle(values);
                //Add the link record
                myRepo.AddLink(values, article_id);
                //Add the digital media record
                myRepo.AddDigitalMedia(values, article_id);
                //Add the nine shares records
                myRepo.AddShares(values, article_id);
                //Add the language record
                int language_id = (int)myRepo.AddLanguage(values, article_id);
                //Add the five lda records
                myRepo.AddLDA(values, language_id);
                //Add the six polarity records
                myRepo.AddPolarity(values, language_id);

                //Increment the count so that we know we are making progress
                count++;
                //The processing of lines takes a while, so provide some feedback
                //so that we know the program is still working
                if (count % 10 == 0)
                {
                    Console.WriteLine("Record " + count + " added.");
                }
                if (count % 1000 == 0)
                {
                    Console.WriteLine("Milestone count achieved: " + count);
                }
            }
            myRepo.CloseConnection();
        }
    }
}
