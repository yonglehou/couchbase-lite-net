/**
 * Couchbase Lite for .NET
 *
 * Original iOS version by Jens Alfke
 * Android Port by Marty Schoch, Traun Leyden
 * C# Port by Zack Gramana
 *
 * Copyright (c) 2012, 2013 Couchbase, Inc. All rights reserved.
 * Portions (c) 2013 Xamarin, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using Couchbase;
using Couchbase.Internal;
using Sharpen;

namespace Couchbase.Internal
{
	public class CBLDatabaseInternal
	{
		private CBLDatabase cblDatabase;

		private SQLiteDatabase sqliteDb;

		public CBLDatabaseInternal(CBLDatabase cblDatabase, SQLiteDatabase sqliteDb)
		{
			this.cblDatabase = cblDatabase;
			this.sqliteDb = sqliteDb;
		}

		public virtual CBLRevisionInternal GetLocalDocument(string docID, string revID)
		{
			CBLRevisionInternal result = null;
			Cursor cursor = null;
			try
			{
				string[] args = new string[] { docID };
				cursor = sqliteDb.RawQuery("SELECT revid, json FROM localdocs WHERE docid=?", args
					);
				if (cursor.MoveToFirst())
				{
					string gotRevID = cursor.GetString(0);
					if (revID != null && (!revID.Equals(gotRevID)))
					{
						return null;
					}
					byte[] json = cursor.GetBlob(1);
					IDictionary<string, object> properties = null;
					try
					{
						properties = CBLServer.GetObjectMapper().ReadValue<IDictionary>(json);
						properties.Put("_id", docID);
						properties.Put("_rev", gotRevID);
						result = new CBLRevisionInternal(docID, gotRevID, false, cblDatabase);
						result.SetProperties(properties);
					}
					catch (Exception e)
					{
						Log.W(CBLDatabase.Tag, "Error parsing local doc JSON", e);
						return null;
					}
				}
				return result;
			}
			catch (SQLException e)
			{
				Log.E(CBLDatabase.Tag, "Error getting local document", e);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual void DeleteLocalDocument(string docID, string revID)
		{
			if (docID == null)
			{
				throw new CBLiteException(CBLStatus.BadRequest);
			}
			if (revID == null)
			{
				// Didn't specify a revision to delete: 404 or a 409, depending
				if (GetLocalDocument(docID, null) != null)
				{
					throw new CBLiteException(CBLStatus.Conflict);
				}
				else
				{
					throw new CBLiteException(CBLStatus.NotFound);
				}
			}
			string[] whereArgs = new string[] { docID, revID };
			try
			{
				int rowsDeleted = sqliteDb.Delete("localdocs", "docid=? AND revid=?", whereArgs);
				if (rowsDeleted == 0)
				{
					if (GetLocalDocument(docID, null) != null)
					{
						throw new CBLiteException(CBLStatus.Conflict);
					}
					else
					{
						throw new CBLiteException(CBLStatus.NotFound);
					}
				}
			}
			catch (SQLException e)
			{
				throw new CBLiteException(e, CBLStatus.InternalServerError);
			}
		}
	}
}
