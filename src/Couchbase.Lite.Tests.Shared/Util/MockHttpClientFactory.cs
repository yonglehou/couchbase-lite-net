﻿//
// MockHttpClientFactory.cs
//
// Author:
//     Pasin Suriyentrakorn  <pasin@couchbase.com>
//
// Copyright (c) 2014 Couchbase Inc
// Copyright (c) 2014 .NET Foundation
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
//
// Copyright (c) 2014 Couchbase, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
//
    
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using Couchbase.Lite.Support;
using Couchbase.Lite.Util;
using Couchbase.Lite.Replicator;

#if NET_3_5
using System.Net.Couchbase;
#else
using System.Net;
#endif

namespace Couchbase.Lite.Tests
{
    public class MockHttpClientFactory : IHttpClientFactory
    {
        const string Tag = "MockHttpClientFactory";

        private readonly CookieStore cookieStore;

        public MockHttpRequestHandler HttpHandler { get; private set;}

        public MessageProcessingHandler Handler {
            get {
                throw new NotImplementedException ();
            }
        }

        public IDictionary<string, string> Headers { get; set; }

        public MockHttpClientFactory(bool defaultFail = true) : this (null, defaultFail) { }

        public MockHttpClientFactory(DirectoryInfo cookieStoreDirectory, bool defaultFail = true)
        {
            cookieStore = new CookieStore(cookieStoreDirectory != null 
                ? cookieStoreDirectory.FullName
                : null);
            HttpHandler = new MockHttpRequestHandler(defaultFail);
            HttpHandler.CookieContainer = cookieStore;
            HttpHandler.UseCookies = true;

            Headers = new Dictionary<string,string>();
        }

        public HttpClient GetHttpClient(bool chunkedMode)
        {
            var client = new HttpClient(HttpHandler, false);

            foreach(var header in Headers)
            {
                var success = client.DefaultRequestHeaders.TryAddWithoutValidation(header.Key, header.Value);
                if (!success)
                {
                    Log.W(Tag, "Unabled to add header to request: {0}: {1}".Fmt(header.Key, header.Value));
                }
            }
            return client;
        }

        public void AddCookies(CookieCollection cookies)
        {
            cookieStore.Add(cookies);
        }

        public void DeleteCookie(Uri uri, string name)
        {
            cookieStore.Delete(uri, name);
        }

        public CookieContainer GetCookieContainer()
        {
            return cookieStore;
        }
    }
}
