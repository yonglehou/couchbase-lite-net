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

using System.Collections.Generic;
using Couchbase;
using Sharpen;

namespace Couchbase
{
	/// <summary>Represents a query of a CouchbaseLite 'view', or of a view-like resource like _all_documents.
	/// 	</summary>
	/// <remarks>Represents a query of a CouchbaseLite 'view', or of a view-like resource like _all_documents.
	/// 	</remarks>
	public class CBLQuery
	{
		public enum CBLStaleness
		{
			CBLStaleNever,
			CBLStaleOK,
			CBLStaleUpdateAfter
		}

		/// <summary>The database that contains this view.</summary>
		/// <remarks>The database that contains this view.</remarks>
		private CBLDatabase database;

		/// <summary>The view object associated with this query</summary>
		private CBLView view;

		/// <summary>Is this query based on a temporary view?</summary>
		private bool temporaryView;

		/// <summary>The number of initial rows to skip.</summary>
		/// <remarks>
		/// The number of initial rows to skip. Default value is 0.
		/// Should only be used with small values. For efficient paging, use startKey and limit.
		/// </remarks>
		private int skip;

		/// <summary>The maximum number of rows to return.</summary>
		/// <remarks>The maximum number of rows to return. Default value is 0, meaning 'unlimited'.
		/// 	</remarks>
		private int limit = int.MaxValue;

		/// <summary>If non-nil, the key value to start at.</summary>
		/// <remarks>If non-nil, the key value to start at.</remarks>
		private object startKey;

		/// <summary>If non-nil, the key value to end after.</summary>
		/// <remarks>If non-nil, the key value to end after.</remarks>
		private object endKey;

		/// <summary>If non-nil, the document ID to start at.</summary>
		/// <remarks>
		/// If non-nil, the document ID to start at.
		/// (Useful if the view contains multiple identical keys, making .startKey ambiguous.)
		/// </remarks>
		private string startKeyDocId;

		/// <summary>If non-nil, the document ID to end at.</summary>
		/// <remarks>
		/// If non-nil, the document ID to end at.
		/// (Useful if the view contains multiple identical keys, making .endKey ambiguous.)
		/// </remarks>
		private string endKeyDocId;

		/// <summary>If set, the view will not be updated for this query, even if the database has changed.
		/// 	</summary>
		/// <remarks>
		/// If set, the view will not be updated for this query, even if the database has changed.
		/// This allows faster results at the expense of returning possibly out-of-date data.
		/// </remarks>
		private CBLQuery.CBLStaleness stale;

		/// <summary>Should the rows be returned in descending key order? Default value is NO.
		/// 	</summary>
		/// <remarks>Should the rows be returned in descending key order? Default value is NO.
		/// 	</remarks>
		private bool descending;

		/// <summary>If set to YES, the results will include the entire document contents of the associated rows.
		/// 	</summary>
		/// <remarks>
		/// If set to YES, the results will include the entire document contents of the associated rows.
		/// These can be accessed via CBLQueryRow's -documentProperties property.
		/// This slows down the query, but can be a good optimization if you know you'll need the entire
		/// contents of each document. (This property is equivalent to "include_docs" in the CouchDB API.)
		/// </remarks>
		private bool prefetch;

		/// <summary>If set to YES, disables use of the reduce function.</summary>
		/// <remarks>
		/// If set to YES, disables use of the reduce function.
		/// (Equivalent to setting "?reduce=false" in the REST API.)
		/// </remarks>
		private bool mapOnly;

		/// <summary>If set to YES, queries created by -queryAllDocuments will include deleted documents.
		/// 	</summary>
		/// <remarks>
		/// If set to YES, queries created by -queryAllDocuments will include deleted documents.
		/// This property has no effect in other types of queries.
		/// </remarks>
		private bool includeDeleted;

		/// <summary>If non-nil, the query will fetch only the rows with the given keys.</summary>
		/// <remarks>If non-nil, the query will fetch only the rows with the given keys.</remarks>
		private IList<object> keys;

		/// <summary>If non-zero, enables grouping of results, in views that have reduce functions.
		/// 	</summary>
		/// <remarks>If non-zero, enables grouping of results, in views that have reduce functions.
		/// 	</remarks>
		private int groupLevel;

		private long lastSequence;

		private CBLStatus status;

		internal CBLQuery(CBLDatabase database, CBLView view)
		{
			// null for _all_docs query
			// Result status of last query (.error property derived from this)
			this.database = database;
			this.view = view;
			limit = int.MaxValue;
			mapOnly = (view.GetReduce() == null);
		}

		internal CBLQuery(CBLDatabase database, CBLMapFunction mapFunction) : this(database
			, database.MakeAnonymousView())
		{
			temporaryView = true;
			view.SetMap(mapFunction, string.Empty);
		}

		internal CBLQuery(CBLDatabase database, Couchbase.CBLQuery query) : this(database
			, query.GetView())
		{
			limit = query.limit;
			skip = query.skip;
			startKey = query.startKey;
			endKey = query.endKey;
			descending = query.descending;
			prefetch = query.prefetch;
			keys = query.keys;
			groupLevel = query.groupLevel;
			mapOnly = query.mapOnly;
			startKeyDocId = query.startKeyDocId;
			endKeyDocId = query.endKeyDocId;
			stale = query.stale;
		}

		/// <summary>Sends the query to the server and returns an enumerator over the result rows (Synchronous).
		/// 	</summary>
		/// <remarks>
		/// Sends the query to the server and returns an enumerator over the result rows (Synchronous).
		/// If the query fails, this method returns nil and sets the query's .error property.
		/// </remarks>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual CBLQueryEnumerator GetRows()
		{
			IList<long> outSequence = new AList<long>();
			IList<CBLQueryRow> rows = database.QueryViewNamed(view.GetName(), GetQueryOptions
				(), outSequence);
			long lastSequence = outSequence[0];
			return new CBLQueryEnumerator(database, rows, lastSequence);
		}

		/// <summary>
		/// Same as -rows, except returns nil if the query results have not changed since the last time it
		/// was evaluated (Synchronous).
		/// </summary>
		/// <remarks>
		/// Same as -rows, except returns nil if the query results have not changed since the last time it
		/// was evaluated (Synchronous).
		/// </remarks>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual CBLQueryEnumerator GetRowsIfChanged()
		{
			if (database.GetLastSequence() == lastSequence)
			{
				return null;
			}
			return GetRows();
		}

		/// <summary>Returns a live query with the same parameters.</summary>
		/// <remarks>Returns a live query with the same parameters.</remarks>
		public virtual CBLLiveQuery ToLiveQuery()
		{
			return new CBLLiveQuery(this);
		}

		/// <summary>Starts an asynchronous query.</summary>
		/// <remarks>
		/// Starts an asynchronous query. Returns immediately, then calls the onLiveQueryChanged block when the
		/// query completes, passing it the row enumerator. If the query fails, the block will receive
		/// a non-nil enumerator but its .error property will be set to a value reflecting the error.
		/// The originating CBLQuery's .error property will NOT change.
		/// </remarks>
		public virtual void RunAsync(CBLQueryCompleteFunction queryCompleteFunction)
		{
			RunAsyncInternal(queryCompleteFunction);
		}

		internal virtual Sharpen.Thread RunAsyncInternal(CBLQueryCompleteFunction queryCompleteFunction
			)
		{
			Sharpen.Thread t = new Sharpen.Thread(new _Runnable_176(this, queryCompleteFunction
				));
			t.Start();
			return t;
		}

		private sealed class _Runnable_176 : Runnable
		{
			public _Runnable_176(CBLQuery _enclosing, CBLQueryCompleteFunction queryCompleteFunction
				)
			{
				this._enclosing = _enclosing;
				this.queryCompleteFunction = queryCompleteFunction;
			}

			public void Run()
			{
				try
				{
					string viewName = this._enclosing.view.GetName();
					CBLQueryOptions options = this._enclosing.GetQueryOptions();
					IList<long> outSequence = new AList<long>();
					IList<CBLQueryRow> rows = this._enclosing.database.QueryViewNamed(viewName, options
						, outSequence);
					long sequenceNumber = outSequence[0];
					CBLQueryEnumerator enumerator = new CBLQueryEnumerator(this._enclosing.database, 
						rows, sequenceNumber);
					queryCompleteFunction.OnQueryChanged(enumerator);
				}
				catch (CBLiteException e)
				{
					queryCompleteFunction.OnFailureQueryChanged(e);
				}
			}

			private readonly CBLQuery _enclosing;

			private readonly CBLQueryCompleteFunction queryCompleteFunction;
		}

		public virtual CBLView GetView()
		{
			return view;
		}

		public virtual CBLDatabase GetDatabase()
		{
			return database;
		}

		public virtual int GetSkip()
		{
			return skip;
		}

		public virtual void SetSkip(int skip)
		{
			this.skip = skip;
		}

		public virtual int GetLimit()
		{
			return limit;
		}

		public virtual void SetLimit(int limit)
		{
			this.limit = limit;
		}

		public virtual bool IsDescending()
		{
			return descending;
		}

		public virtual void SetDescending(bool descending)
		{
			this.descending = descending;
		}

		public virtual object GetStartKey()
		{
			return startKey;
		}

		public virtual void SetStartKey(object startKey)
		{
			this.startKey = startKey;
		}

		public virtual object GetEndKey()
		{
			return endKey;
		}

		public virtual void SetEndKey(object endKey)
		{
			this.endKey = endKey;
		}

		public virtual string GetStartKeyDocId()
		{
			return startKeyDocId;
		}

		public virtual void SetStartKeyDocId(string startKeyDocId)
		{
			this.startKeyDocId = startKeyDocId;
		}

		public virtual string GetEndKeyDocId()
		{
			return endKeyDocId;
		}

		public virtual void SetEndKeyDocId(string endKeyDocId)
		{
			this.endKeyDocId = endKeyDocId;
		}

		public virtual CBLQuery.CBLStaleness GetStale()
		{
			return stale;
		}

		public virtual void SetStale(CBLQuery.CBLStaleness stale)
		{
			this.stale = stale;
		}

		public virtual IList<object> GetKeys()
		{
			return keys;
		}

		public virtual void SetKeys(IList<object> keys)
		{
			this.keys = keys;
		}

		public virtual bool IsPrefetch()
		{
			return prefetch;
		}

		public virtual void SetPrefetch(bool prefetch)
		{
			this.prefetch = prefetch;
		}

		public virtual bool IsMapOnly()
		{
			return mapOnly;
		}

		public virtual void SetMapOnly(bool mapOnly)
		{
			this.mapOnly = mapOnly;
		}

		public virtual bool IsIncludeDeleted()
		{
			return includeDeleted;
		}

		public virtual void SetIncludeDeleted(bool includeDeleted)
		{
			this.includeDeleted = includeDeleted;
		}

		public virtual int GetGroupLevel()
		{
			return groupLevel;
		}

		public virtual void SetGroupLevel(int groupLevel)
		{
			this.groupLevel = groupLevel;
		}

		private CBLQueryOptions GetQueryOptions()
		{
			CBLQueryOptions queryOptions = new CBLQueryOptions();
			queryOptions.SetStartKey(GetStartKey());
			queryOptions.SetEndKey(GetEndKey());
			queryOptions.SetStartKey(GetStartKey());
			queryOptions.SetKeys(GetKeys());
			queryOptions.SetSkip(GetSkip());
			queryOptions.SetLimit(GetLimit());
			queryOptions.SetReduce(!IsMapOnly());
			queryOptions.SetReduceSpecified(true);
			queryOptions.SetGroupLevel(GetGroupLevel());
			queryOptions.SetDescending(IsDescending());
			queryOptions.SetIncludeDocs(IsPrefetch());
			queryOptions.SetUpdateSeq(true);
			queryOptions.SetInclusiveEnd(true);
			queryOptions.SetIncludeDeletedDocs(IsIncludeDeleted());
			queryOptions.SetStale(GetStale());
			return queryOptions;
		}

		~CBLQuery()
		{
			base.Finalize();
			if (temporaryView)
			{
				view.Delete();
			}
		}
	}
}
