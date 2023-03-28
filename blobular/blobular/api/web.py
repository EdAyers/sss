from typing import Any, Optional
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from miniscutil.misc import append_url_params, human_size
from uxu import h, Manager, render_static
import dominate
import dominate.tags as t

from blobular.registry import BlobClaim

from .settings import Settings
from .github_login import login_handler
from .persist import ApiKey as ApiKeyEntry, BlobularApiDatabase as Db, database
from .authentication import try_get_user, User, get_user

router = APIRouter()

"""
Things to make:
- landing page
- login
- list of your blobs -- ideally with some live reloading.
- docs pages
"""


def get_sign_in_url():
    cfg = Settings.current()
    base = "https://github.com/login/oauth/authorize"
    params = {
        "client_id": cfg.github_client_id,
        "redirect_uri": cfg.cloud_url + "/login",
        "scope": "user:email",
    }
    uri = append_url_params(base, **params)
    return uri


@router.get("/login")
async def web_login(
    request: Request, code: str, state: Optional[str] = None, db=Depends(database)
):
    cfg = Settings.current()
    jwt = await login_handler(code, db)
    max_age = int(cfg.jwt_expires.total_seconds())
    domain = "127.0.0.1"  # cfg.cloud_url
    headers = {"Set-Cookie": f"jwt={jwt}; HttpOnly; Max-Age={max_age}; domain={domain}"}
    # [todo] allow redirects to other routes in our domain
    # remember: never allow arbitrary redirects to other domains
    # for now just always redirect to index.
    return RedirectResponse("/", headers=headers, status_code=302)


def layout(content, user: Optional[User] = Depends(try_get_user)):
    # [todo] if user is signed in, show their profile picture.
    url = get_sign_in_url()
    if user is not None:
        user_menu = h(
            "div",
            {"class": 'class="pa4 tc"'},
            h(
                "img",
                {
                    "className": "br-100 h3 w3 dib",
                    "src": user.gh_avatar_url,
                    "alt": user.gh_username,
                },
            ),
        )
    else:
        user_menu = h("a", {"title": "sign in with GitHub", "href": url}, "sign in")
    return h(
        "body",
        {},
        [
            h(
                "header",
                {"class": "flex justify-between items-center"},
                [
                    h("h1", {"class": "f1"}, "BLOBULAR"),
                    user_menu,
                ],
            ),
            h("main", {}, content),
            h(
                "footer",
                {"class": "bt pt3"},
                [
                    h(
                        "a",
                        {
                            "href": "https://github.com/sss/blobular"
                        },  # [todo] get these from pyproject.toml
                        "GitHub Repository",
                    ),
                    h(
                        "a",
                        {"href": "https://pypi.org/project/blobular/"},
                        "PyPA Package",
                    ),
                    h("span", {}, "© 2023 E.W.Ayers"),
                ],
            ),
        ],
    )


def make_the_table(db: Db, user: User):
    # [todo] use dataframes there's no reason not to
    blobs = list(
        db.blobs.select(where=BlobClaim.user_id == user.id, order_by=BlobClaim.created)
    )
    return h(
        "table",
        {},
        h(
            "thead",
            {},
            h(
                "tr",
                {},
                h("th", {}, "Digest"),
                h("th", {}, "Size"),
                h("th", {}, "Accesses"),
                h("th", {}, "Last Accessed"),
                h("th", {}, "Created"),
            ),
        ),
        h(
            "tbody",
            {},
            [
                h(
                    "tr",
                    {},
                    h("td", {}, b.digest[:8]),
                    h("td", {}, human_size(b.content_length)),
                    h("td", {}, str(b.accesses)),
                    h("td", {}, str(b.last_accessed)),
                    h("td", {}, str(b.created)),
                )
                for b in blobs
            ],
        ),
    )


@router.get("/")
async def read_root(user: User = Depends(try_get_user), db: Db = Depends(database)):
    main = [h("p", {}, "Content-addressed filestore for Python.")]
    if user is not None:
        main.append(h("p", {}, f"Signed in as {user.gh_username}."))
        main.append(make_the_table(db, user))
    else:
        main.append(h("p", {}, "Not signed in."))
    root = layout(h("p", {}, *main), user)

    # just do a full loop to get the render.
    r = render_static(root)

    doc: Any = dominate.document(title="Blobular")
    with doc.head:
        t.link(
            rel="stylesheet",
            href="https://unpkg.com/tachyons@4.12.0/css/tachyons.min.css",
        )
        tails = ["", "-aile", "-curly-slab", "-curly", "-etoile", "-slab"]
        for tail in tails:
            font = "iosevka" + tail
            t.link(
                rel="stylesheet",
                href=f"https://cdn.jsdelivr.net/gh/aymanbagabas/iosevka-fonts@v11.1.1/dist/{font}/{font}.min.css",
            )
        t.style(
            "body { font-family: Iosevka Slab Web, monospace;}",
            "th { text-align: left; }",
            "th, td { padding-right: 1rem; }",
        )

    doc.add(r.static())
    doc.body["class"] = "ph6 pt2"
    content = doc.render()
    return HTMLResponse(content=content)
