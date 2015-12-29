using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace GetDataIntoMySQL
{
    /// <summary>
    /// Class designed to manage the interaction between our application
    /// and the MySQL database
    /// </summary>
    class MySQLRepository
    {
        #region Fields

        //The connnection to the database
        private MySqlConnection connection;

        //Cache the values of the ids for days and categories
        //so that we don't have to keep going to the database
        private Dictionary<String, int> days = new Dictionary<string, int>();
        private Dictionary<String, int> categories = new Dictionary<string, int>();

        #endregion

        #region Constructor

        /// <summary>
        /// Construct a new repository and connect to the database
        /// using the password entered by the user
        /// </summary>
        public MySQLRepository(string password)
        {
            string connectionString = ConfigurationManager.ConnectionStrings["MySQL"].ConnectionString;
            connectionString += "PASSWORD=" + password;
            connection = new MySqlConnection(connectionString);
        }

        #endregion

        #region Populate Lookup Tables

        /// <summary>
        /// Populate the 'day' table with the days of the week and
        /// mark Saturday and Sunday as weekend days
        /// </summary>
        public void CreateDays()
        {
            List<string> days = new List<string>(){
                "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
            };

            try
            {
                foreach (var day in days)
                {
                    //Make sure the day doesn't already exist in the database
                    if (!valueExistsInTableColumn(day, "day", "name"))
                    {
                        string query = String.Format("INSERT INTO day (name, is_weekend) VALUES('{0}', {1})",
                            day, day.Equals("Saturday") || day.Equals("Sunday") ? "1" : "0");
                        MySqlCommand cmd = new MySqlCommand(query, connection);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        /// <summary>
        /// Populate the 'category' table.
        /// </summary>
        public void CreateCategories()
        {
            List<string> categories = new List<string>(){
                "Lifestyle", "Entertainment", "Business", "Social Media", "Technology", "World"
            };

            try
            {
                foreach (var category in categories)
                {
                    //Make sure the category doesn't already exist in the database
                    if (!valueExistsInTableColumn(category, "category", "name"))
                    {
                        string query = String.Format("INSERT INTO category (name) VALUES('{0}')", category);
                        MySqlCommand cmd = new MySqlCommand(query, connection);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        /// <summary>
        /// Populate the 'rank' table
        /// </summary>
        public void CreateRanks()
        {
            List<string> ranks = new List<string>(){
                "best", "worst", "average"
            };

            try
            {
                foreach (var rank in ranks)
                {
                    //Make sure the rank doesn't already exist in the database
                    if (!valueExistsInTableColumn(rank, "rank", "description"))
                    {
                        string query = String.Format("INSERT INTO rank (description) VALUES('{0}')", rank);
                        MySqlCommand cmd = new MySqlCommand(query, connection);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        /// <summary>
        /// Populate the classification table
        /// </summary>
        public void CreateClassifications()
        {
            List<string> classifications = new List<string>(){
                "minimum", "average", "maximum"
            };

            try
            {
                foreach (var classification in classifications)
                {
                    //Make sure the classification doesn't already exist in the database
                    if (!valueExistsInTableColumn(classification, "classification", "description"))
                    {
                        string query = String.Format("INSERT INTO classification (description) VALUES('{0}')", classification);
                        MySqlCommand cmd = new MySqlCommand(query, connection);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        /// <summary>
        /// Used to determine if a value is already present in our database
        /// </summary>
        /// <param name="value">The value for which to search</param>
        /// <param name="table">The table to search for value</param>
        /// <param name="column">The column to compare to value</param>
        /// <returns></returns>
        private bool valueExistsInTableColumn(string value, string table, string column)
        {
            int count = 0;
            string query = String.Format("SELECT COUNT(1) FROM {1} WHERE {2} = '{0}'", value, table, column);
            MySqlCommand cmd = new MySqlCommand(query, connection);
            int.TryParse(cmd.ExecuteScalar().ToString(), out count);
            return count == 1;
        }

        #endregion

        #region Populate Tables with Attributes from CSV

        /// <summary>
        /// Add the 'article' record associated with this record in the csv file
        /// </summary>
        /// <param name="values">The pertinent data from the csv</param>
        /// <returns>The article_id of the newly added record</returns>
        public long AddArticle(string[] values)
        {
            int category_id = GetCategoryID(values);
            int day_id = GetDayID(values);

            string query = String.Format("INSERT INTO article "
            + "(category_id, num_keywords, n_tokens_title, n_tokens_content, average_token_length, n_non_stop_words, n_unique_tokens, n_non_stop_unique_tokens, day_id, shares) "
            + "VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}')",
                category_id, values[12], values[2], values[3], values[11], values[5], values[4], values[6], day_id, values[60]);
            return ExecuteNonQuery(query);
        }

        /// <summary>
        /// Add the 'link' record associated with this record in the csv file
        /// </summary>
        /// <param name="values">The pertinent data from the csv</param>
        /// <param name="article_id">The id associated with the FK to the article table</param>
        public void AddLink(string[] values, int article_id)
        {
            string query = String.Format("INSERT INTO link "
            + "(article_id, num_hrefs, num_self_hrefs, self_reference_min_shares, self_reference_avg_shares, self_reference_max_shares) "
            + "VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}')",
                article_id, values[7], values[8], values[28], values[29], values[30]);
            ExecuteNonQuery(query);
        }

        /// <summary>
        /// Add the 'digital_media' record associated with this record in the csv file
        /// </summary>
        /// <param name="values">The pertinent data from the csv</param>
        /// <param name="article_id">The id associated with the FK to the article table</param>
        public void AddDigitalMedia(string[] values, int article_id)
        {
            string query = String.Format("INSERT INTO digital_media "
            + "(article_id, num_imgs, num_videos) "
            + "VALUES('{0}', '{1}', '{2}')",
                article_id, values[9], values[10]);
            ExecuteNonQuery(query);
        }

        /// <summary>
        /// Adds nine 'share' records for each record in the csv
        /// worst_minimum
        /// worst_average
        /// worst_maximum
        /// average_minimum
        /// average_average
        /// average_maximum
        /// best_minimum
        /// best_average
        /// best_maximum
        /// </summary>
        /// <param name="values">The pertinent data from the csv</param>
        /// <param name="article_id">The id associated with the FK to the article table</param>
        public void AddShares(string[] values, int article_id)
        {
            string query = String.Format("INSERT INTO share "
            + "(article_id, rank, classification, n_shares) "
            + "VALUES('{0}', '{1}', '{2}', '{3}')",
                article_id, "worst", "minimum", values[19]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO share "
            + "(article_id, rank, classification, n_shares) "
            + "VALUES('{0}', '{1}', '{2}', '{3}')",
                article_id, "worst", "maximum", values[20]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO share "
            + "(article_id, rank, classification, n_shares) "
            + "VALUES('{0}', '{1}', '{2}', '{3}')",
                article_id, "worst", "average", values[21]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO share "
            + "(article_id, rank, classification, n_shares) "
            + "VALUES('{0}', '{1}', '{2}', '{3}')",
                article_id, "best", "minimum", values[22]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO share "
            + "(article_id, rank, classification, n_shares) "
            + "VALUES('{0}', '{1}', '{2}', '{3}')",
                article_id, "best", "maximum", values[23]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO share "
            + "(article_id, rank, classification, n_shares) "
            + "VALUES('{0}', '{1}', '{2}', '{3}')",
                article_id, "best", "average", values[24]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO share "
            + "(article_id, rank, classification, n_shares) "
            + "VALUES('{0}', '{1}', '{2}', '{3}')",
                article_id, "average", "minimum", values[25]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO share "
            + "(article_id, rank, classification, n_shares) "
            + "VALUES('{0}', '{1}', '{2}', '{3}')",
                article_id, "average", "maximum", values[26]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO share "
            + "(article_id, rank, classification, n_shares) "
            + "VALUES('{0}', '{1}', '{2}', '{3}')",
                article_id, "average", "average", values[27]);
            ExecuteNonQuery(query);
        }

        /// <summary>
        /// Add the 'language' record associated with this record in the csv file
        /// </summary>
        /// <param name="values">The pertinent data from the csv</param>
        /// <param name="article_id">The id associated with the FK to the article table</param>
        /// <returns>The language_id of the newly inserted record</returns>
        public long AddLanguage(string[] values, int article_id)
        {
            string query = String.Format("INSERT INTO language "
            + "(title_subjectivity, global_subjectivity, abs_title_subjectivity, title_sentiment_polarity, global_rate_positive_words, global_rate_negative_words, rate_positive_words, rate_negative_words, global_sentiment_polarity, abs_title_sentiment_polarity, article_id) "
            + "VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}')",
                values[56], values[44], values[58], values[57], values[46], values[47], values[48], values[49], values[45], values[59], article_id);
            return ExecuteNonQuery(query);
        }

        /// <summary>
        /// Adds five 'lda' records for each record in the csv
        /// </summary>
        /// <param name="values">The pertinent data from the csv</param>
        /// <param name="language_id">The id associated with the FK to the language table</param>
        public void AddLDA(string[] values, int language_id)
        {
            string query = String.Format("INSERT INTO lda "
            + "(language_id, n_topic, ratio) "
            + "VALUES('{0}', '{1}', '{2}')",
                language_id, "0", values[39]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO lda "
            + "(language_id, n_topic, ratio) "
            + "VALUES('{0}', '{1}', '{2}')",
                language_id, "1", values[40]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO lda "
            + "(language_id, n_topic, ratio) "
            + "VALUES('{0}', '{1}', '{2}')",
                language_id, "2", values[41]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO lda "
            + "(language_id, n_topic, ratio) "
            + "VALUES('{0}', '{1}', '{2}')",
                language_id, "3", values[42]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO lda "
            + "(language_id, n_topic, ratio) "
            + "VALUES('{0}', '{1}', '{2}')",
                language_id, "4", values[43]);
            ExecuteNonQuery(query);
        }

        /// <summary>
        /// Creates six polarity records for each record in the csv
        /// positive_minimum
        /// positive_average
        /// positive_maximum
        /// negative_minimum
        /// negative_average
        /// negative_maximum
        /// </summary>
        /// <param name="values">The pertinent data from the csv</param>
        /// <param name="language_id">The id associated with the FK to the language table</param>
        public void AddPolarity(string[] values, int language_id)
        {
            string query = String.Format("INSERT INTO polarity "
            + "(language_id, classification, ratio, is_positive) "
            + "VALUES('{0}', '{1}', '{2}', 1)",
                language_id, "average", values[50]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO polarity "
            + "(language_id, classification, ratio, is_positive) "
            + "VALUES('{0}', '{1}', '{2}', 1)",
                language_id, "minimum", values[51]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO polarity "
            + "(language_id, classification, ratio, is_positive) "
            + "VALUES('{0}', '{1}', '{2}', 1)",
                language_id, "maximum", values[52]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO polarity "
            + "(language_id, classification, ratio, is_positive) "
            + "VALUES('{0}', '{1}', '{2}', 0)",
                language_id, "average", values[53]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO polarity "
            + "(language_id, classification, ratio, is_positive) "
            + "VALUES('{0}', '{1}', '{2}', 0)",
                language_id, "minimum", values[54]);
            ExecuteNonQuery(query);

            query = String.Format("INSERT INTO polarity "
            + "(language_id, classification, ratio, is_positive) "
            + "VALUES('{0}', '{1}', '{2}', 0)",
                language_id, "maximum", values[55]);
            ExecuteNonQuery(query);
        }

        #endregion

        #region Helper Methods for Maintaining Referential Integrity

        /// <summary>
        /// Gets the id associated with the given day
        /// </summary>
        /// <param name="values">Bit columns for which one should be true</param>
        /// <returns>The day id associated with the true bit column</returns>
        public int GetDayID(string[] values)
        {
            int day_id = 0;
            if (values[31].Trim().Equals("1.0"))
            {
                day_id = getIDWhereColumnHasValue("Monday", "day", "name", "day_id");
            }
            else if (values[32].Trim().Equals("1.0"))
            {
                day_id = getIDWhereColumnHasValue("Tuesday", "day", "name", "day_id");
            }
            else if (values[33].Trim().Equals("1.0"))
            {
                day_id = getIDWhereColumnHasValue("Wednesday", "day", "name", "day_id");
            }
            else if (values[34].Trim().Equals("1.0"))
            {
                day_id = getIDWhereColumnHasValue("Thursday", "day", "name", "day_id");
            }
            else if (values[35].Trim().Equals("1.0"))
            {
                day_id = getIDWhereColumnHasValue("Friday", "day", "name", "day_id");
            }
            else if (values[36].Trim().Equals("1.0"))
            {
                day_id = getIDWhereColumnHasValue("Saturday", "day", "name", "day_id");
            }
            else if (values[37].Trim().Equals("1.0"))
            {
                day_id = getIDWhereColumnHasValue("Sunday", "day", "name", "day_id");
            }

            return day_id;
        }

        /// <summary>
        /// Gets the id associated with the given category
        /// </summary>
        /// <param name="values">Bit columns for which one should be true</param>
        /// <returns>The category id associated with the true bit column</returns>
        public int GetCategoryID(string[] values)
        {
            int category_id = 0;
            if (values[13].Trim().Equals("1.0"))
            {
                //Get the category_id for Lifestyle
                category_id = getIDWhereColumnHasValue("Lifestyle", "category", "name", "category_id");
            }
            else if (values[14].Trim().Equals("1.0"))
            {
                //Get the category_id for Entertainment
                category_id = getIDWhereColumnHasValue("Entertainment", "category", "name", "category_id");
            }
            else if (values[15].Trim().Equals("1.0"))
            {
                //Get the category_id for Business
                category_id = getIDWhereColumnHasValue("Business", "category", "name", "category_id");
            }
            else if (values[16].Trim().Equals("1.0"))
            {
                //Get the category_id for Social Media
                category_id = getIDWhereColumnHasValue("Social Media", "category", "name", "category_id");
            }
            else if (values[17].Trim().Equals("1.0"))
            {
                //Get the category_id for Technology
                category_id = getIDWhereColumnHasValue("Technology", "category", "name", "category_id");
            }
            else if (values[18].Trim().Equals("1.0"))
            {
                //Get the category_id for World
                category_id = getIDWhereColumnHasValue("World", "category", "name", "category_id");
            }

            return category_id;
        }

        /// <summary>
        /// If our dictionary of values already contains the ID for 'value' then return it.
        /// Otherwise populate the dictionary of possible values, then lookup the ID.
        /// </summary>
        /// <param name="value">The value to search for</param>
        /// <param name="table">The table to search</param>
        /// <param name="columnToMatch">The column containing the value</param>
        /// <param name="idCol">The id column</param>
        /// <returns>The value of the id column where table.columnToMatch == value</returns>
        public int getIDWhereColumnHasValue(string value, string table, string columnToMatch, string idCol)
        {
            try 
            {
                if(table.Equals("day"))
                {
                    if(days.Count == 0)
                    {
                        PopulateDictionary(table, idCol);
                    }
                    return days[value];
                }
                else if (table.Equals("category"))
                {
                    if(categories.Count == 0)
                    {
                        PopulateDictionary(table, idCol);
                    }
                    return categories[value];
                }
                else
                {
                    throw new ArgumentOutOfRangeException(value);
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
            return 0;
        }

        /// <summary>
        /// Populate the dictionary so that we can lookup the associated ids
        /// without having to repeatedly query the database
        /// </summary>
        private void PopulateDictionary(string table, string idCol)
        {
            
            if ((connection != null && connection.State == System.Data.ConnectionState.Open) || OpenConnection() == true)
            {
                string query = "Select " + idCol + ", name from " + table;
                MySqlCommand cmd = new MySqlCommand(query, connection);
                MySqlDataReader dataReader = cmd.ExecuteReader();
                var myDictionary = table.Equals("day") ? days : categories;
                while (dataReader.Read())
                {
                    int idVal = (int)dataReader[idCol];
                    string name = (string)dataReader["name"];
                    if (!myDictionary.ContainsKey(name))
                    {
                        myDictionary[name] = idVal;
                    }
                }
                //Close the connection
                CloseConnection();
            }
            else
            {
                Console.WriteLine("Connection not open");
            }
        }

        #endregion

        #region Generic MySQL Helper Methods

        /// <summary>
        /// Executes the nonquery.  Opens a new connection if one is not available.
        /// </summary>
        /// <param name="query">Update, Insert, or Delete to execute</param>
        /// <returns>The last inserted ID when applicable</returns>
        private long ExecuteNonQuery(string query)
        {
            try
            {
                if ((connection != null && connection.State == System.Data.ConnectionState.Open) || OpenConnection() == true)
                {
                    MySqlCommand cmd = new MySqlCommand(query, connection);
                    cmd.ExecuteNonQuery();
                    long id = cmd.LastInsertedId;
                    return id;
                }
                else
                {
                    Console.WriteLine("Connection not open");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            return -1;
        }

        /// <summary>
        /// Open the connection to the database
        /// </summary>
        /// <returns>True if successful</returns>
        public bool OpenConnection()
        {
            try
            {
                connection.Open();
                return true;
            }
            catch (MySqlException ex)
            {
                switch (ex.Number)
                {
                    case 0:
                        Console.WriteLine("Cannot connect to server.  Contact administrator");
                        break;
                    case 1045:
                        Console.WriteLine("Invalid username/password, please try again");
                        break;
                }
                return false;
            }
        }

        /// <summary>
        /// Close the connection to the database
        /// </summary>
        /// <returns>True if successful</returns>
        public bool CloseConnection()
        {
            try
            {
                connection.Close();
                return true;
            }
            catch (MySqlException ex)
            {
                Console.WriteLine(ex.Message);
                return false;
            }
        }

        #endregion
    }
}
