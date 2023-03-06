import * as esbuild from 'esbuild'
import * as http from 'http'

/* Reference:
- https://esbuild.github.io/api/#customizing-server-behavior
- https://github.com/nativew/esbuild-serve/blob/3332704d6108b6b1cc7002121babc1355be3a92d/src/index.js
- https://github.com/evanw/esbuild/issues/802#issuecomment-819578182
- https://github.com/evanw/esbuild/issues/802#issuecomment-955776480 version without esbuild.serve
*/

export async function serve(buildOptions, serveOptions) {
    const proxyPort = serveOptions.port || 3000
    const parentOnRebuild = buildOptions?.watch?.onRebuild
    const clients = []
    const eventName = serveOptions.eventName || '/onReload'
    buildOptions = {
        ...buildOptions,
        banner: { js: `(() => new EventSource("http://localhost:${proxyPort}${eventName}").onmessage = () => {console.log('reload triggered'); location.reload()})();` },
        watch: {
            onRebuild(error, result) {
                if (parentOnRebuild) {
                    parentOnRebuild(error, result)
                }
                if (error) {
                    console.error(error)
                    return
                }
                console.log('telling the webview to reload')
                clients.forEach(res => res.write('data: update\n\n'))
                clients.length = 0
            }
        }
    }
    await esbuild.build(buildOptions)

    const { host, port, wait, stop } = await esbuild.serve({
        servedir: '.'
    }, {})
    // Then start a proxy server on proxyPort
    http.createServer((req, res) => {
        if (req.url.endsWith(eventName)) {
            if (req.method === "OPTIONS") {
                console.log('doing cors BS')
                // CORS BS
                const headers = {};
                headers["Access-Control-Allow-Origin"] = "*";
                headers["Access-Control-Allow-Methods"] = "POST, GET, PUT, DELETE, OPTIONS";
                // headers["Access-Control-Allow-Credentials"] = false;
                headers["Access-Control-Max-Age"] = '86400'; // 24 hours
                headers["Access-Control-Allow-Headers"] = "X-Requested-With, X-HTTP-Method-Override, Content-Type, Accept, Cache-Control";
                res.writeHead(200, headers);
                res.end('')
                return
            }
            console.log('subscription to reloads', req.method)
            clients.push(res.writeHead(200, {
                'Content-Type': 'text/event-stream',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                "Access-Control-Allow-Origin": "*",
            }))
            return
        }

        const options = {
            hostname: host,
            port: port,
            path: req.url,
            method: req.method,
            headers: req.headers,
        }

        // Forward each incoming request to esbuild
        const proxyReq = http.request(options, proxyRes => {
            // If esbuild returns "not found", send a custom 404 page
            if (proxyRes.statusCode === 404) {
                console.log(`404 for ${req.url}`)
                res.writeHead(404, { 'Content-Type': 'text/html' });
                res.end('<h1>A custom 404 page</h1>');
                return;
            }

            // Otherwise, forward the response from esbuild to the client
            res.writeHead(proxyRes.statusCode, proxyRes.headers);
            proxyRes.pipe(res, { end: true });
        });

        // Forward the body of the request to esbuild
        req.pipe(proxyReq, { end: true });
    }).listen(proxyPort);
}
