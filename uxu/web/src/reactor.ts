
/* React-free implementation. */

import { JsonRpc } from "./rpc"

interface EventHandler {
    '__handler__': string
}

interface RenderedText {
    kind: 'text'
    id: string
    value: string
}
interface RenderedWidget {
    kind: 'widget'
    id: string
    name: string
    props: any
}
interface RenderedElement {
    kind: 'element'
    tag: string
    id: string;
    attrs: { [key: string]: string | EventHandler | any };
    children: Rendering[]
}
interface RenderedFragment {
    kind: 'fragment'
    id: string
    children: Rendering[]
}
interface RootRendering {
    id: string
    kind: 'root'
    children: Rendering[]
    key?: string
}

const WIDGETS = new Map<string, WidgetSpec<any>>()

export function registerWidget(spec: WidgetSpec<any>) {
    return WIDGETS.set(spec.name, spec)
}

interface WidgetSpec<P> {
    name: string
    create(props: P): Element
    reconcile(previous_node: Element, previous_props: P, new_props: P): void
    dispose(node: Element): void
}

type Rendering = RenderedText | RenderedElement | RenderedFragment | RenderedWidget

interface ModifyChildrenPatch {
    kind: 'modify-children'
    element_id: string;
    children_length_start: number;
    remove_these: Map<number, null | string>
    then_insert_these: Map<number, [0, string] | [1, Rendering]>
}

interface ModifyAttributesPatch {
    kind: 'modify-attrs'
    add: { [key: string]: any };
    remove: string[];
    element_id: string;
}

interface ReplaceElementPatch {
    kind: 'replace-element'
    new_element: Rendering
    element_id: string;
}

interface ReplaceRootPatch {
    kind: 'replace-root'
    root: RootRendering
}

interface InvalidatePatch {
    kind: 'invalidate'
}

type Patch = ModifyChildrenPatch | ModifyAttributesPatch | ReplaceElementPatch | ReplaceRootPatch | InvalidatePatch;

interface VdomNode {
    kind: 'text' | 'element' | 'fragment'
    id: string;
    node: Node;
    handlers: Map<string, any>
    child_ids: string[];
}

interface VdomWidget {
    kind: 'widget'
    props: any
    spec: WidgetSpec<any>
    id: string;
    node: Element;
    handlers: Map<string, any>
    child_ids: string[];
}

type Vdom = VdomNode | VdomWidget;

/* [todo] current impl is overcomplicated, we can just store the ids
   on the DOM using data-uxu-id attribute.
   Only thing to figure out is fragments and addressing Text nodes.
*/

class DomManager {
    nodes: Map<string, Vdom> = new Map();
    parents: Map<string, string> = new Map();
    mountPoint: Element;
    root: VdomNode
    onEvent: any
    constructor(mountPoint: Element, onEvent: any, initial?: RootRendering) {
        this.mountPoint = mountPoint;
        this.onEvent = onEvent;
        this.root = {
            kind: 'element',
            id: 'root',
            node: mountPoint,
            handlers: new Map(),
            child_ids: [],
        }
        this.setVdom(this.root)
        if (initial) {
            this.replaceRoot(initial);
        }
    }

    private getFragmentIdx(id: string): number {
        let v = this.nodes.get(id)!
        if (v.kind === 'element' || v.kind === 'text') {
            return 0
        } else if (v.kind === 'fragment') {
            const parent_id = this.parents.get(v.id)
            if (!parent_id) { return 0 }
            const parent = this.nodes.get(parent_id)!
            const cidx = parent.child_ids.indexOf(v.id)
            if (cidx === -1) { throw new Error(`Fragment ${v.id} not found in parent ${parent_id}`) }
            return cidx + this.getFragmentIdx(parent_id)
        } else {
            throw new Error(`Unknown kind ${v.kind}`)
        }
    }
    private getFragmentElementParent(parent_id: string): Vdom {
        let v = this.nodes.get(parent_id)!
        if (v.kind === 'fragment') {
            return this.getFragmentElementParent(this.parents.get(v.id)!)
        } else {
            return v
        }
    }

    private setVdom(node: Vdom) {
        for (const child_id of node.child_ids) {
            this.parents.set(child_id, node.id);
        }
        this.nodes.set(node.id, node);
    }
    private removeVdom(node: Vdom) {
        for (const child_id of node.child_ids) {
            this.parents.delete(child_id);
        }
        this.nodes.delete(node.id);
    }

    setAttr(vn: VdomNode, key: string, value: any) {
        const elt: Element = vn.node as any;
        if (typeof value == 'string') {
            elt.setAttribute(key, value);
        } else if ('__handler__' in value) {
            const handler_id = value['__handler__'];
            const handler = (args: any) => this.onEvent({ element_id: vn.id, handler_id, name: key, params: args })
            vn.handlers.set(key, handler)
            elt.addEventListener(key, handler)
        } else if (key == 'style') {
            for (const sk in value) {
                const sv = value[sk];
                // @ts-ignore
                elt.style[sk] = sv;
            }
        } else {
            throw new Error(`Unknown attribute: ${key}`);
        }
    }

    rmAttr(vn: VdomNode, key: string) {
        const elt: Element = vn.node as any;
        if (vn.handlers.has(key)) {
            const handler = vn.handlers.get(key)!;
            elt.removeEventListener(key, handler)
            vn.handlers.delete(key)
        } else {
            elt.removeAttribute(key);
        }
    }

    create(x: Rendering, parent_id: string): Vdom {
        if (x.kind == 'element') {
            const elt = document.createElement(x.tag);
            const vn: VdomNode = { id: x.id, node: elt, handlers: new Map(), child_ids: x.children.map(c => c.id), kind: 'element' };
            this.setVdom(vn)
            for (const key in x.attrs) {
                const val = x.attrs[key];
                this.setAttr(vn, key, val);
            }
            for (const child of x.children) {
                const cn = this.create(child, x.id)
                elt.appendChild(cn.node);
            }
            this.setVdom(vn)
            return vn
        } else if (x.kind == 'text') {
            const text = document.createTextNode(x.value);
            const vn: VdomNode = { id: x.id, node: text, handlers: new Map(), child_ids: [], kind: 'text' }
            this.setVdom(vn);
            return vn
        } else if (x.kind === 'fragment') {
            const parent_vn = this.getFragmentElementParent(parent_id)
            const elt: Element = parent_vn.node as any
            const vn: VdomNode = { id: x.id, node: elt, handlers: new Map(), child_ids: x.children.map(c => c.id), kind: 'fragment' }
            this.setVdom(vn)
            const fidx = this.getFragmentIdx(x.id)
            const sib = elt.childNodes[fidx]
            for (const child of x.children) {
                const cn = this.create(child, x.id)
                if (sib) {
                    sib.after(cn.node)
                } else {
                    elt.insertBefore(cn.node, null)
                }
            }
            this.setVdom(vn)
            return vn
        } else if (x.kind === "widget") {
            if (!WIDGETS.has(x.name)) {
                throw new Error(`Unknown widget: ${x.name}`)
            }
            const spec = WIDGETS.get(x.name)!
            const elt = spec.create(x.props)
            const vn: Vdom = { id: x.id, node: elt, handlers: new Map(), child_ids: [], kind: 'widget', props: x.props, spec }
            this.setVdom(vn)
            return vn
        } else {
            // @ts-ignore
            throw new Error(`Unknown kind: ${x.kind}`);
        }
    }

    remove(id: string) {
        const vn = this.nodes.get(id);
        if (!vn) { return undefined; }
        if (vn.kind === "element") {
            for (const key in vn.handlers) {
                this.rmAttr(vn, key)
            }
        }
        if (vn.kind === 'widget') {
            vn.spec.dispose(vn.node)
        }
        this.removeVdom(vn);
        vn.child_ids.forEach(x => this.remove(x))
        return vn
    }

    replaceRoot(root: RootRendering): void {
        this.remove(this.root.id)!
        this.root = {
            kind: 'element',
            id: root.id,
            node: this.mountPoint,
            handlers: new Map(),
            child_ids: [],
        }
        this.setVdom(this.root)
        const cs = root.children.map(x => this.create(x, this.root.id));
        this.mountPoint.replaceChildren(...cs.map(x => x.node));
        this.root.child_ids = cs.map(x => x.id);
        this.setVdom(this.root)
    }

    replaceElement(patch: ReplaceElementPatch): void {
        const rv = this.remove(patch.element_id)!
        const parent_id = this.parents.get(rv.id)!
        if (rv.kind !== 'element') {
            throw new Error(`Invalid kind: ${rv.kind}`);
        }
        const node1: Element = rv.node as any
        const rv2 = this.create(patch.new_element, parent_id)
        node1.replaceWith(rv2.node)
        const parent = this.nodes.get(parent_id)!
        const siblings = parent.child_ids
        const cidx = siblings.indexOf(rv.id)
        if (cidx === -1) { throw new Error(`Element ${rv.id} not found in parent ${parent_id}`) }
        siblings[cidx] = rv2.id
        this.setVdom(parent)
        this.setVdom(rv2)
    }

    modifyAttributes(patch: ModifyAttributesPatch): void {
        const vn = this.nodes.get(patch.element_id)!
        if (vn.kind === 'widget') {
            throw new Error(`Invalid kind: ${vn.kind}`);
        } else {
            for (const attr of patch.remove) {
                this.rmAttr(vn, attr)
            }
            for (const key in patch.add) {
                const val = patch.add[key]
                this.setAttr(vn, key, val)
            }
        }
    }

    modifyChildren(patch: ModifyChildrenPatch): void {
        const fidx = this.getFragmentIdx(patch.element_id)
        const vn = this.nodes.get(patch.element_id)!
        const elt: Element = vn.node as any
        if (elt.childNodes.length < patch.children_length_start) {
            console.error(`modifyChildren: ${elt} has ${elt.childNodes.length} children but was expected to have at least ${patch.children_length_start}`)
        }
        const cvns = vn.child_ids.map(id => this.nodes.get(id)!)
        const bucket = new Map()
        const removals = Array.from(patch.remove_these.entries()).sort((a, b) => a[0] - b[0])
        for (const [i, v] of removals) {
            const child_node = elt.childNodes[i + fidx]
            const cvn = cvns[i]
            if (child_node !== cvn.node) {
                throw new Error(`modifyChildren ${i}: ${child_node} is not ${cvn.node}`)
            }
            elt.removeChild(child_node)
            if (typeof v === 'string') {
                bucket.set(v, cvn)
            } else {
                this.remove(cvn.id)
            }
        }
        vn.child_ids = vn.child_ids.filter((id, i) => !patch.remove_these.has(i))
        const insertions = Array.from(patch.then_insert_these.entries()).sort((a, b) => b[0] - a[0])
        for (const [j, kv] of insertions) {
            const cb = (j + fidx) >= elt.childNodes.length ? null : elt.childNodes[j + fidx]
            const new_vn: VdomNode = kv[0] === 0 ? bucket.get(kv[1]) : this.create(kv[1], vn.id)
            this.setVdom(new_vn)
            elt.insertBefore(new_vn.node, cb)
            vn.child_ids.splice(j, 0, new_vn.id)
        }
        this.setVdom(vn)
    }

    async applyPatches(patches: Patch[]) {
        for (const patch of patches) {
            switch (patch.kind) {
                case 'modify-attrs':
                    this.modifyAttributes(patch)
                    break;
                case 'modify-children':
                    this.modifyChildren(patch)
                    break;
                case 'replace-root':
                    this.replaceRoot(patch.root)
                    break;
                case 'replace-element':
                    this.replaceElement(patch)
                    break;
                default:
                    throw new Error(`Unknown kind: ${patch.kind}`);
            }
        }
    }
}

export class ReactorApp {
    rpc: JsonRpc
    mgr: DomManager

    constructor(sock: WebSocket, mount: Element, readonly ticket: string) {
        sock.addEventListener('open', () => this.onOpen())
        this.rpc = new JsonRpc(sock)
        this.rpc.register('patch', (p) => this.onPatch(p))
        this.mgr = new DomManager(mount, (x: any) => this.rpc.notify('event', x))
    }

    async onOpen() {
        console.log(`initializing`);
        const resp = await this.rpc.request("initialize", {
            clientInfo: {
                name: "uxu-app",
                version: "0.0.0", // [todo] get version
            },
            ticket: this.ticket,
            url: window.location.href,
            // [todo] authentication?
        });
        console.log("initialize response:", resp);
        this.rpc.notify("initialized", {});
        this.rerender();
    }

    async rerender() {
        console.log('rerender')
        const r: RootRendering = await this.rpc.request('render', {})
        this.mgr.replaceRoot(r)
    }

    async onPatch(patches: Patch[]) {
        // [todo] should include patch versioning to make sure in sync
        if (patches.some(p => p.kind === "invalidate")) {
            await this.rerender()
            return "invalidated"
        } else {
            try {
                await this.mgr.applyPatches(patches)
                return "success"
            }
            catch (e) {
                console.error(e)
                await this.rerender()
                return "error"
            }
        }
    }
}