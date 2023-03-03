from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from uxu import h, Manager, render_static
import dominate

router = APIRouter()


@router.get("/")
async def read_root():
    root = h(
        "main", {}, h("h1", {}, "Hello from Blobular!"), h("p", {}, "please like me.")
    )

    # just do a full loop to get the render.
    rs = render_static(root)

    doc = dominate.document(title="Blobular")
    for r in rs:
        doc.add(r.static())
    content = doc.render()
    return HTMLResponse(content=content)
