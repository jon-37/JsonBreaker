using System.Data;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;



namespace JsonBreaker
{
    public class JsonBreaker
    {
        private List<IndexType> indexes = new List<IndexType>();
        private List<IndexType> ind = new List<IndexType>();
        static private int index = 0, order = 1;

        public DataSet JsonToDataTable(string json, string tableName = "Root")
        {
            index = 0; order = 1;
            DataSet dataSet = new DataSet();
            var jToken = JToken.Parse(json);

            if (jToken.Type == JTokenType.Object) //single json object 
            {
                indexes.Add(new IndexType()
                {
                    Name = tableName,
                    TableName = tableName,
                    Index = index,
                    Order = order,
                    Level = tableName.Count(t => t == '\\'),
                    Count = jToken.Count(),
                    Type = jToken.Type.ToString()
                });
                order++;
                ParseJObject(dataSet, jToken.ToObject<JObject>(), tableName, tableName);
            }
            else //multiple json objects in array 
            {
                foreach (var jObject in jToken.Children<JObject>())
                {
                    indexes.Add(new IndexType()
                    {
                        Name = tableName,
                        TableName = tableName,
                        Index = index,
                        Order = order,
                        Level = tableName.Count(t => t == '\\'),
                        Count = jToken.Count(),
                        Type = jToken.Type.ToString()
                    });
                    order++;
                    ParseJObject(dataSet, jObject, tableName, tableName);
                }
            }
            foreach (var a in indexes.OrderByDescending(i => i.Level).OrderByDescending(l => l.Order).OrderBy(k => k.Index))
            {
                if (dataSet.Tables[a.TableName].Columns.Contains(a.Name.Replace(a.TableName + "\\", "")) && a.Index <= dataSet.Tables[a.TableName].Columns.Count - 1)
                {
                    dataSet.Tables[a.TableName].Columns[a.Name.Replace(a.TableName + "\\", "")].SetOrdinal(a.Index);
                }
                else if (dataSet.Tables[a.TableName].Columns.Contains(a.Name.Replace(a.TableName + "\\", "")) && a.Index > dataSet.Tables[a.TableName].Columns.Count - 1)
                {
                    dataSet.Tables[a.TableName].Columns[a.Name.Replace(a.TableName + "\\", "")].SetOrdinal(dataSet.Tables[a.TableName].Columns.Count - 1);
                }
            }
            for (int i = 0; i < dataSet.Tables.Count; i++)
            {
                if (dataSet.Tables[i].Columns.Contains("BaseVersion"))
                {

                    string baseVersionJson = dataSet.Tables[i].Rows[0]["BaseVersion"].ToString();
                    var baseVersionJToken = JToken.Parse(baseVersionJson);
                    indexes.Add(new IndexType()
                    {
                        Name = "Root\\BaseVersion",
                        TableName = "Root\\BaseVersion",
                        Index = index,
                        Order = order,
                        Level = 1,
                        Count = baseVersionJToken.Count(),
                        Type = jToken.Type.ToString()
                    });
                    ParseJObject(dataSet, baseVersionJToken.ToObject<JObject>(), "Root\\BaseVersion", "Root\\BaseVersion");
                }
            }
            return dataSet;
        }
        private void ParseJObject(DataSet dataSet, JObject jObject, string tableName, string parentTableName)
        {
            int level = 0, count = 0, counter = 0;
            if (indexes.Count > 0)
            {
                count = indexes.Last().Count;
                level = indexes.Last().Level;
                if ((count != jObject.Count && level == tableName.Count(t => t == '\\') && indexes.Last().TableName == tableName))
                {
                    indexes.RemoveAll(i => i.TableName == tableName);
                }
            }

            foreach (var ob in jObject)
            {
                string name = tableName + "\\" + ob.Key;

                if (dataSet.Tables.Contains(name))
                {
                    indexes.Add(new IndexType()
                    {
                        Name = name,
                        TableName = tableName,
                        Index = counter,
                        Order = order,
                        Level = tableName.Count(t => t == '\\'),
                        Count = ob.Value.Count(),
                        TableCountSource = dataSet.Tables[name].Rows.Count,
                        TableCountDestination = dataSet.Tables[tableName].Rows.Count,
                        Type = jObject[ob.Key].Type.ToString()
                    });
                    counter++;
                }
                else
                {
                    indexes.Add(new IndexType()
                    {
                        Name = name,
                        TableName = tableName,
                        Index = counter,
                        Order = order,
                        Level = name.Count(t => t == '\\'),
                        Count = ob.Value.Count(),
                        Type = jObject[ob.Key].Type.ToString()
                    });
                    counter++;
                }
            }

            if (jObject.Count >= 0)
            {
                DataTable dt = new DataTable(tableName);
                Dictionary<string, dynamic> dic = new Dictionary<string, dynamic>();
                List<TableColumn> listColumns = new List<TableColumn>();
                foreach (JProperty property in jObject.Properties())
                {
                    string key = property.Name;
                    JToken jToken = property.Value;

                    if (jToken.Type == JTokenType.Object)
                    {
                        var jo = jToken.ToObject<JObject>();

                        if (!ind.Where(i => i.Name == tableName + "\\" + key).Any())
                        {
                            ind.Add(new IndexType()
                            {
                                Name = tableName + "\\" + key,
                                TableName = tableName,
                                Order = order,
                                Level = tableName.Count(t => t == '\\')
                            });                           
                        }
                        ParseJObject(dataSet, jo, tableName + "\\" + key, tableName);
                        order++;
                    }
                    else if (jToken.Type == JTokenType.Array)
                    {
                        var arrs = jToken.ToObject<JArray>();
                        var objects = arrs.Children<JObject>();
                        if (objects.Count() > 0) 
                        {
                            foreach (var arr in objects)
                            {
                                var jo = arr.ToObject<JObject>();
                                if (!ind.Where(i => i.Name == tableName + "\\" + key).Any())
                                {
                                    ind.Add(new IndexType()
                                    {
                                        Name = tableName + "\\" + key,
                                        TableName = tableName,
                                        Order = order,
                                        Level = tableName.Count(t => t == '\\')
                                    });
                                    order++;
                                }
                                ParseJObject(dataSet, jo, tableName + "\\" + key, tableName);
                            }
                        }                       
                        else
                        {
                            listColumns.Add(new TableColumn { Name = key, Value = string.Join(",", arrs.ToObject<string[]>()), Type = GetJtokenType(jToken.Type.ToString()) });

                            dic[key] = string.Join(",", arrs.ToObject<string[]>());
                        }
                    }
                    else
                    {
                        listColumns.Add(new TableColumn { Name = key, Value = jToken.ToString(), Type = GetJtokenType(jToken.Type.ToString()) });
                        if (jToken.Type.ToString() == "Null")
                        {
                            dic[key] = null;
                            indexes[indexes.FindIndex(i => i.Name.Replace(tableName + "\\", "") == tableName)].nullIgnore = 0;
                        }
                        else
                        {
                            dic[key] = jToken.ToString();
                        }
                    }
                }

                if (dataSet.Tables.Contains(dt.TableName)) //for array items
                {
                    int i = 0;
                    foreach (string key in dic.Keys)
                    {
                        if (!dataSet.Tables[dt.TableName].Columns.Contains(key))
                        {
                            dataSet.Tables[dt.TableName].Columns.Add(AddColumn(key, listColumns[i].Type));
                        }
                        i++;
                    }

                    DataRow dr = dataSet.Tables[dt.TableName].NewRow();
                    foreach (string key in dic.Keys)
                    {
                        dr[key] = dic[key];
                    }

                    dataSet.Tables[dt.TableName].Rows.Add(dr);

                }
                else if (dic.Keys.Count >= 0)
                {
                    List<string> keylist = new List<string>();
                    keylist.AddRange(dic.Keys.ToList());
                    int i = 0;
                    foreach (var key in keylist)
                    {
                        dt.Columns.Add(AddColumn(dic.Keys.ToArray()[keylist.IndexOf(key)], listColumns[i].Type));
                        i++;
                    }

                    if (dic.Values.Count() > 0)
                    {
                        dt.Rows.Add(dic.Values.ToArray());
                    }
                    else
                    {
                        dt.Rows.Add();
                    }

                    dataSet.Tables.Add(dt);
                }
            }
        }
        private DataColumn AddColumn(string name, string type)
        {
            return new DataColumn()
            {
                ColumnName = name,
                DataType = System.Type.GetType(type),
                AutoIncrementSeed = 1,
                AutoIncrementStep = 1,
                AllowDBNull = true
            };
        }

        public string DataSetToJson(DataSet dataSet)
        {
            int level = indexes.Max(i => i.Level);
            string resultJson = "", tableName = "", Name = "";
            Dictionary<string, int> tableCountsInit = new Dictionary<string, int>();
            Dictionary<string, int> tableCountsFinal = new Dictionary<string, int>();
            foreach (DataTable ct in dataSet.Tables)
            {
                tableCountsInit.Add(ct.TableName.ToString(), ct.Rows.Count);
                tableCountsFinal.Add(ct.TableName.ToString(), ct.Rows.Count);
            }
            while (indexes.Count() > 0)
            {
                foreach (IndexType indi in indexes.OrderBy(k => k.Index).OrderByDescending(i => i.Level).OrderByDescending(l => l.Order))
                {
                    if (indi.TableName == "Root\\Changes")
                    {
                        level = 0;
                    }
                    Name = indi.Name;
                    tableName = indi.TableName;
                    if (!indexes.Contains(indi))
                    {
                        continue;
                    }

                    if (!dataSet.Tables.Contains(indi.Name) && !dataSet.Tables[tableName].Columns.Contains(indi.Name))
                    {
                        if (!dataSet.Tables[indi.TableName].Columns.Contains(Name.Replace(indi.TableName + "\\", "")))
                        {
                            dataSet.Tables[indi.TableName].Columns.Add(Name.Replace(indi.TableName + "\\", ""));
                            if (indi.Index > dataSet.Tables[indi.TableName].Columns.Count - 1)
                            {
                                dataSet.Tables[indi.TableName].Columns[Name.Replace(indi.TableName + "\\", "")].SetOrdinal(dataSet.Tables[indi.TableName].Columns.Count - 1);
                            }
                            else
                            {
                                dataSet.Tables[indi.TableName].Columns[Name.Replace(indi.TableName + "\\", "")].SetOrdinal(indi.Index);
                            }
                        }

                        indexes.Remove(indi);
                        continue;
                    }
                    var settings = new JsonSerializerSettings
                    {
                        NullValueHandling = NullValueHandling.Ignore,
                        Converters = { new DataRowConverter() },
                    };
                    int tci = tableCountsFinal[Name] == 0 ? 0 : tableCountsFinal[Name] - 1;

                    if (indi.Name == indi.TableName && indexes.Count==1)
                    {
                        resultJson = JsonConvert.SerializeObject(dataSet.Tables[Name].Rows, Formatting.None, settings);
                        resultJson = Regex.Unescape(resultJson);
                        resultJson = resultJson.Replace("\"[{}]\"", "{}").Replace("\"[", "[").Replace("]\"", "]").Replace("\"{", "{").Replace("}\"", "}");
                        resultJson = resultJson.Remove(0, 1).Remove(resultJson.Length - 2, 1);

                    }
                    else
                    {
                        if (indi.Name == indi.TableName)
                        {
                            string pseudoResult = JsonConvert.SerializeObject(dataSet.Tables[Name].Rows[indi.TableCountSource], Formatting.None, settings);
                            pseudoResult = Regex.Unescape(pseudoResult);
                            if (!dataSet.Tables["Root"].Columns.Contains(Name.Replace("Root\\", "")))
                            {
                                dataSet.Tables["Root"].Columns.Add(Name.Replace("Root\\", ""));
                                if (indi.Index > dataSet.Tables["Root"].Columns.Count - 1)
                                {
                                    dataSet.Tables["Root"].Columns[Name.Replace("Root\\", "")].SetOrdinal(dataSet.Tables["Root"].Columns.Count - 1);
                                }
                                else
                                {
                                    dataSet.Tables["Root"].Columns[Name.Replace("Root\\", "")].SetOrdinal(indi.Index);
                                }
                            }
                            if (Name.Replace("Root\\", "") == "BaseVersion")
                            {
                                pseudoResult = "\"" + pseudoResult + "\"";
                                indexes.RemoveAll(i => i.Name == "Root\\BaseVersion");
                            }
                            dataSet.Tables["Root"].Rows[indi.TableCountDestination][Name.Replace("Root\\", "")] = pseudoResult;
                        }
                        else if (indi.TableCountSource == indi.TableCountDestination && indi.Count > 0 && indi.TableCountSource == indi.Count)
                        {
                            string pseudoResult = JsonConvert.SerializeObject(dataSet.Tables[Name].Rows[indi.TableCountSource], Formatting.None, settings);
                            pseudoResult = Regex.Unescape(pseudoResult);
                            if (!dataSet.Tables[indi.TableName].Columns.Contains(Name.Replace(indi.TableName + "\\", "")))
                            {
                                dataSet.Tables[indi.TableName].Columns.Add(Name.Replace(indi.TableName + "\\", ""));
                                if (indi.Index > dataSet.Tables[indi.TableName].Columns.Count - 1)
                                {
                                    dataSet.Tables[indi.TableName].Columns[Name.Replace(indi.TableName + "\\", "")].SetOrdinal(dataSet.Tables[indi.TableName].Columns.Count - 1);
                                }
                                else
                                {
                                    dataSet.Tables[indi.TableName].Columns[Name.Replace(indi.TableName + "\\", "")].SetOrdinal(indi.Index);
                                }
                            }
                            dataSet.Tables[indi.TableName].Rows[indi.TableCountDestination][Name.Replace(indi.TableName + "\\", "")] = pseudoResult;

                            tableCountsFinal[tableName]--;
                            tableCountsFinal[Name]--;
                        }
                        else
                        {

                            if (indi.Count > 0)
                            {
                                string pseudoResult = "";
                                for (int j = tableCountsInit[Name] - 1; j >= indi.TableCountSource; j--)
                                {
                                    pseudoResult = JsonConvert.SerializeObject(dataSet.Tables[Name].Rows[j], Formatting.None, settings) + (pseudoResult == "" ? pseudoResult : "," + pseudoResult);
                                    tableCountsFinal[Name]--;

                                }
                                tableCountsInit[Name] = indi.TableCountSource;
                                if (!dataSet.Tables[indi.TableName].Columns.Contains(Name.Replace(indi.TableName + "\\", "")))
                                {
                                    dataSet.Tables[indi.TableName].Columns.Add(Name.Replace(indi.TableName + "\\", ""));
                                    if (indi.Index > dataSet.Tables[indi.TableName].Columns.Count - 1)
                                    {
                                        dataSet.Tables[indi.TableName].Columns[Name.Replace(indi.TableName + "\\", "")].SetOrdinal(dataSet.Tables[indi.TableName].Columns.Count - 1);
                                    }
                                    else
                                    {
                                        dataSet.Tables[indi.TableName].Columns[Name.Replace(indi.TableName + "\\", "")].SetOrdinal(indi.Index);
                                    }
                                }
                                pseudoResult = Regex.Unescape(pseudoResult);
                                if (indi.Type == "Array")
                                {
                                    pseudoResult = pseudoResult.Insert(0, "[").Insert(pseudoResult.Length + 1, "]");
                                }
                                dataSet.Tables[indi.TableName].Rows[indi.TableCountDestination][Name.Replace(indi.TableName + "\\", "")] = pseudoResult;
                                tableCountsFinal[tableName]--;
                                if (dataSet.Tables.Contains(indi.Name))
                                {
                                    indexes.Remove(indi);
                                }
                                continue;
                            }
                            else
                            {
                                string pseudoResult = indi.Type == "Array" ? "[]" : "{}";

                                if (!dataSet.Tables[indi.TableName].Columns.Contains(Name.Replace(indi.TableName + "\\", "")))
                                {
                                    dataSet.Tables[indi.TableName].Columns.Add(Name.Replace(indi.TableName + "\\", ""));
                                    if (indi.Index > dataSet.Tables[indi.TableName].Columns.Count - 1)
                                    {
                                        dataSet.Tables[indi.TableName].Columns[Name.Replace(indi.TableName + "\\", "")].SetOrdinal(dataSet.Tables[indi.TableName].Columns.Count - 1);
                                    }
                                    else
                                    {
                                        dataSet.Tables[indi.TableName].Columns[Name.Replace(indi.TableName + "\\", "")].SetOrdinal(indi.Index);
                                    }
                                }
                                pseudoResult = Regex.Unescape(pseudoResult);
                                dataSet.Tables[indi.TableName].Rows[indi.TableCountDestination][Name.Replace(indi.TableName + "\\", "")] = pseudoResult;
                                tableCountsFinal[tableName]--;
                                tableCountsFinal[Name]--;
                            }

                        }
                    }
                    if (dataSet.Tables.Contains(indi.Name))
                    {
                        indexes.Remove(indi);
                    }
                }
            }
            return resultJson;
        }
        private string GetJtokenType(string type)
        {
            string tokenType = "";
            switch (type)
            {
                case "Integer":
                    tokenType = "System.Int32";
                    break;
                case "Float":
                    tokenType = "System.Single";
                    break;
                case "String":
                    tokenType = "System.String";
                    break;
                case "Boolean":
                    tokenType = "System.Boolean";
                    break;
                case "Date":
                    tokenType = "System.DateTime";
                    break;
                case "Uri":
                    tokenType = "System.String";
                    break;
                default:
                    tokenType = "System.String";
                    break;
            }
            return tokenType;
        }

        public class DataRowConverter : JsonConverter<DataRow>
        {
            public override DataRow ReadJson(JsonReader reader, Type objectType, DataRow existingValue, bool hasExistingValue, JsonSerializer serializer)
            {
                throw new NotImplementedException(string.Format("{0} is only implemented for writing.", this));
            }

            public override void WriteJson(JsonWriter writer, DataRow row, JsonSerializer serializer)
            {
                var table = row.Table;
                if (table == null)
                    throw new JsonSerializationException("no table");
                var contractResolver = serializer.ContractResolver as DefaultContractResolver;

                writer.WriteStartObject();
                foreach (DataColumn col in row.Table.Columns)
                {
                    var value = row[col];
                    if (serializer.NullValueHandling == NullValueHandling.Ignore && (value == null || value == DBNull.Value))
                        continue;
                    writer.WritePropertyName(contractResolver != null ? contractResolver.GetResolvedPropertyName(col.ColumnName) : col.ColumnName);
                    serializer.Serialize(writer, value);
                }
                writer.WriteEndObject();
            }
        }

        internal class IndexType
        {
            public string Name { get; set; }
            public string TableName { get; set; }
            public int Index { get; set; }
            public int Order { get; set; }
            public int Level { get; set; }
            public int Count { get; set; }
            public int nullIgnore { get; set; } = 1;
            public int TableCountSource { get; set; }
            public int TableCountDestination { get; set; }
            public string Type { get; set; }
        }
        internal class TableColumn
        {
            public string Name { get; set; }
            public string Value { get; set; }
            public string Type { get; set; }
        }
    }
}
