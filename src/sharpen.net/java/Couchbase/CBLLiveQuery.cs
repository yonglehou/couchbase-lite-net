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
using System.Collections.Generic;
using Android.Util;
using Couchbase;
using Sharpen;

namespace Couchbase
{
	/// <summary>A CBLQuery subclass that automatically refreshes the result rows every time the database changes.
	/// 	</summary>
	/// <remarks>
	/// A CBLQuery subclass that automatically refreshes the result rows every time the database changes.
	/// All you need to do is use add a listener to observe changes to the .rows property.
	/// </remarks>
	public class CBLLiveQuery : CBLQuery, CBLDatabaseChangedFunction
	{
		private bool observing;

		private bool willUpdate;

		private CBLQueryEnumerator rows;

		private IList<CBLLiveQueryChangedFunction> observers = new AList<CBLLiveQueryChangedFunction
			>();

		private Sharpen.Thread updaterThread;

		internal CBLLiveQuery(CBLQuery query) : base(query.GetDatabase(), query.GetView()
			)
		{
			SetLimit(query.GetLimit());
			SetSkip(query.GetSkip());
			SetStartKey(query.GetStartKey());
			SetEndKey(query.GetEndKey());
			SetDescending(query.IsDescending());
			SetPrefetch(query.IsPrefetch());
			SetKeys(query.GetKeys());
			SetGroupLevel(query.GetGroupLevel());
			SetMapOnly(query.IsMapOnly());
			SetStartKeyDocId(query.GetStartKeyDocId());
			SetEndKeyDocId(query.GetEndKeyDocId());
			SetStale(query.GetStale());
		}

		/// <summary>Starts observing database changes.</summary>
		/// <remarks>
		/// Starts observing database changes. The .rows property will now update automatically. (You
		/// usually don't need to call this yourself, since calling rows()
		/// call start for you.)
		/// </remarks>
		public virtual void Start()
		{
			if (!observing)
			{
				observing = true;
				GetDatabase().AddChangeListener(this);
			}
			Update();
		}

		/// <summary>Stops observing database changes.</summary>
		/// <remarks>Stops observing database changes. Calling start() or rows() will restart it.
		/// 	</remarks>
		public virtual void Stop()
		{
			if (observing)
			{
				observing = false;
				GetDatabase().RemoveChangeListener(this);
			}
			if (willUpdate)
			{
				SetWillUpdate(false);
			}
		}

		// TODO: how can we cancelPreviousPerformRequestsWithTarget ? as done in iOS?
		/// <summary>In CBLLiveQuery the rows accessor is a non-blocking property.</summary>
		/// <remarks>
		/// In CBLLiveQuery the rows accessor is a non-blocking property.
		/// Its value will be nil until the initial query finishes.
		/// </remarks>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public override CBLQueryEnumerator GetRows()
		{
			if (rows == null)
			{
				return null;
			}
			else
			{
				// Have to return a copy because the enumeration has to start at item #0 every time
				return new CBLQueryEnumerator(rows);
			}
		}

		/// <summary>Blocks until the intial async query finishes.</summary>
		/// <remarks>Blocks until the intial async query finishes. After this call either .rows or .error will be non-nil.
		/// 	</remarks>
		public virtual bool WaitForRows()
		{
			Start();
			WaitForUpdateThread();
			return rows != null;
		}

		public virtual void AddChangeListener(CBLLiveQueryChangedFunction liveQueryChangedFunction
			)
		{
			observers.AddItem(liveQueryChangedFunction);
		}

		public virtual void RemoveChangeListener(CBLLiveQueryChangedFunction liveQueryChangedFunction
			)
		{
			observers.Remove(liveQueryChangedFunction);
		}

		internal virtual void Update()
		{
			SetWillUpdate(false);
			updaterThread = RunAsyncInternal(new _CBLQueryCompleteFunction_100(this));
		}

		private sealed class _CBLQueryCompleteFunction_100 : CBLQueryCompleteFunction
		{
			public _CBLQueryCompleteFunction_100(CBLLiveQuery _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void OnQueryChanged(CBLQueryEnumerator queryEnumerator)
			{
				if (queryEnumerator != null && !queryEnumerator.Equals(this._enclosing.rows))
				{
					this._enclosing.SetRows(queryEnumerator);
					foreach (CBLLiveQueryChangedFunction observer in this._enclosing.observers)
					{
						observer.OnLiveQueryChanged(queryEnumerator);
					}
				}
			}

			public void OnFailureQueryChanged(CBLiteException exception)
			{
				foreach (CBLLiveQueryChangedFunction observer in this._enclosing.observers)
				{
					observer.OnFailureLiveQueryChanged(exception);
				}
			}

			private readonly CBLLiveQuery _enclosing;
		}

		public virtual void OnDatabaseChanged(CBLDatabase database)
		{
			if (!willUpdate)
			{
				SetWillUpdate(true);
				// wait for any existing updates to finish before starting
				// a new one.  TODO: this whole class needs review and solid testing
				WaitForUpdateThread();
				Update();
			}
		}

		public virtual void OnFailureDatabaseChanged(CBLiteException exception)
		{
			Log.E(CBLDatabase.Tag, "onFailureDatabaseChanged", exception);
		}

		private void SetRows(CBLQueryEnumerator queryEnumerator)
		{
			lock (this)
			{
				rows = queryEnumerator;
			}
		}

		private void SetWillUpdate(bool willUpdateParam)
		{
			lock (this)
			{
				willUpdate = willUpdateParam;
			}
		}

		private void WaitForUpdateThread()
		{
			if (updaterThread != null)
			{
				try
				{
					updaterThread.Join();
				}
				catch (Exception)
				{
				}
			}
		}
	}
}
