using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GetDataIntoMySQL
{
    class CSVReader
    {
        /// <summary>
        /// Class used to read the csv input file and write to the database
        /// </summary>
        public void ReadFile()
        {
            //The path of the csv file
            string filePath = AppDomain.CurrentDomain.BaseDirectory + @"OnlineNewsPopularity.csv";
            //Read the file line by line
            var reader = new StreamReader(File.OpenRead(filePath));
            if(!reader.EndOfStream)
            {
                //Discard the line containing the headers
                reader.ReadLine();
            }

            //Create a MySQL repository to interact with the database
            MySQLRepository myRepo = new MySQLRepository();
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
                //Add the shares record
                myRepo.AddShares(values, article_id);
                //Add the language record
                int language_id = (int)myRepo.AddLanguage(values, article_id);
                //Add the lda records
                myRepo.AddLDA(values, language_id);
                //Add the polarity records
                myRepo.AddPolarity(values, language_id);

                //Increment the count so that we know we are making progress
                count++;
                //The processing of lines takes a while, so provide some feedback
                //so that we know the program is still working
                if (count % 10 == 0)
                {
                    Console.WriteLine("Record " + count + " added.");
                }
                if(count % 1000 == 0)
                {
                    Console.WriteLine("Milestone count achieved: " + count);
                }
            }
            myRepo.CloseConnection();
        }
    }
}
