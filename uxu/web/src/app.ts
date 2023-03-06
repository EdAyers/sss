import { ReactorApp } from './reactor'
declare const UXU_URL: string;
declare const UXU_TICKET: string;

const sock = new WebSocket(UXU_URL);
const mount = document.getElementById("uxu_root")!;
const reactor = new ReactorApp(sock, mount, UXU_TICKET);
