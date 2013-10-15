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
using System.IO;
using Android.Net;
using Couchbase;
using Couchbase.Internal;
using Sharpen;

namespace Couchbase
{
	public class CBLAttachment
	{
		/// <summary>The owning document revision.</summary>
		/// <remarks>The owning document revision.</remarks>
		private CBLRevisionBase revision;

		/// <summary>The owning document.</summary>
		/// <remarks>The owning document.</remarks>
		private CBLDocument document;

		/// <summary>The filename.</summary>
		/// <remarks>The filename.</remarks>
		private string name;

		/// <summary>The CouchbaseLite metadata about the attachment, that lives in the document.
		/// 	</summary>
		/// <remarks>The CouchbaseLite metadata about the attachment, that lives in the document.
		/// 	</remarks>
		private IDictionary<string, object> metadata;

		/// <summary>The body data.</summary>
		/// <remarks>The body data.</remarks>
		private InputStream body;

		/// <summary>Public Constructor</summary>
		public CBLAttachment(InputStream contentStream, string contentType)
		{
			this.body = contentStream;
			metadata = new Dictionary<string, object>();
			metadata.Put("content_type", contentType);
			metadata.Put("follows", true);
		}

		/// <summary>Package Private Constructor</summary>
		internal CBLAttachment(CBLRevisionBase revision, string name, IDictionary<string, 
			object> metadata)
		{
			this.revision = revision;
			this.name = name;
			this.metadata = metadata;
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual InputStream GetBody()
		{
			if (body != null)
			{
				return body;
			}
			else
			{
				CBLDatabase db = revision.GetDatabase();
				Couchbase.CBLAttachment attachment = db.GetAttachmentForSequence(revision.GetSequence
					(), this.name);
				body = attachment.GetBody();
				return body;
			}
		}

		/// <summary>Get the MIME type of the contents.</summary>
		/// <remarks>Get the MIME type of the contents.</remarks>
		public virtual string GetContentType()
		{
			return (string)metadata.Get("content_type");
		}

		public virtual CBLDocument GetDocument()
		{
			return document;
		}

		public virtual CBLRevisionBase GetRevision()
		{
			return revision;
		}

		public virtual string GetName()
		{
			return name;
		}

		internal virtual void SetName(string name)
		{
			this.name = name;
		}

		internal virtual void SetRevision(CBLRevisionBase revision)
		{
			this.revision = revision;
		}

		/// <summary>Get the length in bytes of the contents.</summary>
		/// <remarks>Get the length in bytes of the contents.</remarks>
		public virtual long GetLength()
		{
			long length = (long)metadata.Get("length");
			if (length != null)
			{
				return length;
			}
			else
			{
				return 0;
			}
		}

		public virtual IDictionary<string, object> GetMetadata()
		{
			return Sharpen.Collections.UnmodifiableMap(metadata);
		}

		/// <summary>Get the URL of the file containing the body.</summary>
		/// <remarks>
		/// Get the URL of the file containing the body.
		/// This is read-only! DO NOT MODIFY OR DELETE THIS FILE.
		/// </remarks>
		public virtual Uri GetBodyURL()
		{
			try
			{
				CBLDatabase db = revision.GetDatabase();
				string filePath = db.GetAttachmentPathForSequence(revision.GetSequence(), name);
				if (filePath != null && filePath.Length > 0)
				{
					return Uri.FromFile(new FilePath(filePath));
				}
				return null;
			}
			catch (CBLiteException e)
			{
				throw new RuntimeException(e);
			}
		}

		internal virtual InputStream GetBodyIfNew()
		{
			return body;
		}

		/// <summary>Updates the body, creating a new document revision in the process.</summary>
		/// <remarks>
		/// Updates the body, creating a new document revision in the process.
		/// If all you need to do to a document is update a single attachment this is an easy way
		/// to do it; but if you need to change multiple attachments, or change other body
		/// properties, do them in one step by calling putProperties on the revision
		/// or document.
		/// </remarks>
		/// <param name="body">The new body, or nil to delete the attachment.</param>
		/// <param name="contentType">The new content type, or nil to leave it the same.</param>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual CBLRevision Update(InputStream body, string contentType)
		{
			CBLDatabase db = revision.GetDatabase();
			CBLRevisionInternal newRevisionInternal = db.UpdateAttachment(name, body, contentType
				, revision.GetDocument().GetId(), revision.GetId());
			return new CBLRevision(document, newRevisionInternal);
		}

		/// <summary>
		/// Goes through an _attachments dictionary and replaces any values that are CBLAttachment objects
		/// with proper JSON metadata dicts.
		/// </summary>
		/// <remarks>
		/// Goes through an _attachments dictionary and replaces any values that are CBLAttachment objects
		/// with proper JSON metadata dicts. It registers the attachment bodies with the blob store and sets
		/// the metadata 'digest' and 'follows' properties accordingly.
		/// </remarks>
		internal static IDictionary<string, object> InstallAttachmentBodies(IDictionary<string
			, object> attachments, CBLDatabase database)
		{
			IDictionary<string, object> updatedAttachments = new Dictionary<string, object>();
			foreach (string name in attachments.Keys)
			{
				object value = attachments.Get(name);
				if (value is Couchbase.CBLAttachment)
				{
					Couchbase.CBLAttachment attachment = (Couchbase.CBLAttachment)value;
					IDictionary<string, object> metadata = attachment.GetMetadata();
					InputStream body = attachment.GetBodyIfNew();
					if (body != null)
					{
						// Copy attachment body into the database's blob store:
						CBLBlobStoreWriter writer = BlobStoreWriterForBody(body, database);
						metadata.Put("length", writer.GetLength());
						metadata.Put("digest", writer.MD5DigestString());
						metadata.Put("follows", true);
						database.RememberAttachmentWriter(writer);
					}
					updatedAttachments.Put(name, metadata);
				}
				else
				{
					if (value is CBLAttachmentInternal)
					{
						throw new ArgumentException("CBLAttachmentInternal objects not expected here.  Could indicate a bug"
							);
					}
				}
			}
			return updatedAttachments;
		}

		internal static CBLBlobStoreWriter BlobStoreWriterForBody(InputStream body, CBLDatabase
			 database)
		{
			CBLBlobStoreWriter writer = database.GetAttachmentWriter();
			writer.Read(body);
			writer.Finish();
			return writer;
		}
	}
}
