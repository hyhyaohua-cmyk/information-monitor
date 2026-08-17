#!/usr/bin/env python3
"""Run one monitor category in GitHub Actions and build a static report."""

from __future__ import annotations

import argparse
import html
import json
import os
import time
from pathlib import Path

import app


ROOT = Path(__file__).resolve().parent
PUBLIC = ROOT / "public"


def run_category(category: str) -> None:
    app.init_db()
    started, run_id = app.start_refresh(category)
    if not started:
        raise RuntimeError("已有刷新任务正在运行")
    while app.refresh_state["running"]:
        message = app.refresh_state.get("message", "")
        percent = app.refresh_state.get("percent", 0)
        print(f"[{percent:>3}%] {message}", flush=True)
        time.sleep(2)
    with app.connect() as db:
        run = db.execute("SELECT status,new_count,ok_count,error_count FROM runs WHERE id=?", (run_id,)).fetchone()
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    if not run or run["status"] != "done":
        raise RuntimeError(f"刷新失败：{app.refresh_state.get('message', '')}")
    print(f"完成：新增 {run['new_count']}，成功通道 {run['ok_count']}，失败通道 {run['error_count']}")


def build_static_report() -> None:
    data = app.state_payload()
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    actions_url = f"{server}/{repository}/actions/workflows/news-monitor.yml" if repository else "#"
    page = f'''<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>信息链接监控报告</title><style>
:root{{--bg:#f4f6f8;--panel:#fff;--ink:#17212b;--muted:#687581;--line:#dfe5ea;--blue:#1769e0}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font:14px/1.55 system-ui,-apple-system,"PingFang SC",sans-serif}}
header{{background:#17212b;color:#fff;padding:20px max(20px,calc((100vw - 1180px)/2));display:flex;align-items:center;justify-content:space-between;gap:18px}}
h1{{font-size:21px;margin:0}}header p{{margin:4px 0 0;opacity:.75}}a{{color:#125dbb;text-decoration:none}}header a{{color:#fff;background:var(--blue);padding:9px 13px;border-radius:8px;white-space:nowrap}}
main{{max-width:1180px;margin:22px auto;padding:0 18px}}.summary{{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}}.card,.panel{{background:#fff;border:1px solid var(--line);border-radius:11px}}.card{{padding:15px}}.card b{{font-size:23px;display:block}}.muted{{color:var(--muted)}}
.toolbar{{display:flex;gap:9px;flex-wrap:wrap;padding:12px;border-bottom:1px solid var(--line)}}input,select{{border:1px solid var(--line);border-radius:7px;padding:8px;background:#fff;min-width:170px}}.tabs{{display:flex;gap:7px;margin:18px 0 9px}}button{{border:1px solid var(--line);border-radius:8px;background:#fff;padding:8px 13px;cursor:pointer}}button.active{{background:#17212b;color:#fff}}.panel{{overflow:hidden}}.hidden{{display:none}}
table{{width:100%;border-collapse:collapse}}th,td{{padding:10px 12px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}}th{{font-size:12px;color:var(--muted);background:#fafbfc}}.pill{{display:inline-block;border-radius:16px;padding:2px 8px;background:#e9ecef;font-size:12px}}.url{{font-size:12px;word-break:break-all}}.title{{font-weight:650}}.empty{{padding:45px;text-align:center;color:var(--muted)}}
@media(max-width:760px){{.summary{{grid-template-columns:1fr 1fr}}header{{align-items:flex-start;flex-direction:column}}main{{padding:0 9px}}th,td{{padding:8px}}}}
</style></head><body><header><div><h1>信息链接监控报告</h1><p id="updated">正在读取报告…</p></div><a href="{html.escape(actions_url)}">前往 Actions 手动刷新</a></header>
<main><section class="summary"><div class="card"><b id="siteCount">—</b><span class="muted">监控来源</span></div><div class="card"><b id="channelCount">—</b><span class="muted">采集通道</span></div><div class="card"><b id="baselineCount">—</b><span class="muted">已建立基线</span></div><div class="card"><b id="reportCount">—</b><span class="muted">近3天新增</span></div></section>
<div class="tabs"><button class="active" data-view="reports">新增报告</button><button data-view="sites">来源状态</button><button data-view="runs">刷新记录</button></div>
<section id="reports" class="panel"><div class="toolbar"><input id="search" placeholder="搜索标题、网址或来源"><select id="category"><option value="">全部类别</option><option>新闻</option><option>智库</option><option>央行</option></select><select id="site"><option value="">全部来源</option></select><select id="run"><option value="">全部刷新时间</option></select></div><div id="reportBody"></div></section>
<section id="sites" class="panel hidden"><div id="siteBody"></div></section><section id="runs" class="panel hidden"><div id="runBody"></div></section></main>
<script id="monitor-data" type="application/json">{payload}</script><script>
const data=JSON.parse(document.querySelector('#monitor-data').textContent),esc=s=>String(s??'').replace(/[&<>"']/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[c])),when=s=>s?new Date(s).toLocaleString('zh-CN',{{hour12:false}}):'—';
const latest=data.runs[0];updated.textContent=latest?`最近刷新：【${{latest.category||'新闻'}}】${{when(latest.finished_at||latest.started_at)}} · 新增 ${{latest.new_count}}`:'尚未刷新';siteCount.textContent=data.sites.length;channelCount.textContent=data.channels.length;baselineCount.textContent=data.channels.filter(x=>x.baseline_at).length;reportCount.textContent=data.reports.length;
site.innerHTML='<option value="">全部来源</option>'+data.sites.map(x=>`<option value="${{esc(x.id)}}">【${{esc(x.category)}}】${{esc(x.name)}}</option>`).join('');run.innerHTML='<option value="">全部刷新时间</option>'+data.runs.map(x=>`<option value="${{esc(x.id)}}">【${{esc(x.category||'新闻')}}】${{when(x.started_at)}} · 新增 ${{x.new_count}}</option>`).join('');
function table(head,rows){{if(!rows.length)return'<div class="empty">暂无记录</div>';return`<table><thead><tr>${{head.map(x=>`<th>${{x}}</th>`).join('')}}</tr></thead><tbody>${{rows.map(r=>`<tr>${{r.map(x=>`<td>${{x}}</td>`).join('')}}</tr>`).join('')}}</tbody></table>`}}
function reports(){{const q=search.value.trim().toLowerCase(),cat=category.value,sid=site.value,rid=run.value;const rows=data.reports.filter(x=>(!cat||x.category===cat)&&(!sid||x.site_id===sid)&&(!rid||x.run_id===rid)&&(!q||`${{x.title_zh}} ${{x.title}} ${{x.url}} ${{x.site_name}}`.toLowerCase().includes(q))).map(x=>[`<div class="title"><a href="${{esc(x.url)}}">${{esc(x.title_zh||x.title||'标题暂不可用')}}</a></div>${{x.title&&x.title!==x.title_zh?`<div>${{esc(x.title)}}</div>`:''}}<div class="url muted">${{esc(x.url)}}</div>`,`<span>${{esc(x.site_name)}}</span><br><span class="pill">${{esc(x.category)}}</span>`,when(x.created_at)]);reportBody.innerHTML=table(['中文标题 / 原标题 / 链接','来源','发现时间'],rows)}}
siteBody.innerHTML=table(['类别','来源','基线/通道','错误','最近成功'],data.sites.map(x=>[`<span class="pill">${{esc(x.category)}}</span>`,`<a href="${{esc(x.home_url)}}">${{esc(x.name)}}</a>`,`${{x.baseline_count||0}} / ${{x.channel_count||0}}`,x.error_count||0,when(x.last_ok_at)]));runBody.innerHTML=table(['类别','开始时间','状态','新增','成功/失败通道'],data.runs.map(x=>[`<span class="pill">${{esc(x.category||'新闻')}}</span>`,when(x.started_at),esc(x.status),x.new_count,`${{x.ok_count}} / ${{x.error_count}}`]));
[search,category,site,run].forEach(x=>x.oninput=reports);document.querySelectorAll('.tabs button').forEach(b=>b.onclick=()=>{{document.querySelectorAll('.tabs button').forEach(x=>x.classList.toggle('active',x===b));['reports','sites','runs'].forEach(id=>document.querySelector('#'+id).classList.toggle('hidden',id!==b.dataset.view))}});reports();
</script></body></html>'''
    PUBLIC.mkdir(exist_ok=True)
    (PUBLIC / "index.html").write_text(page, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", choices=app.CATEGORIES, default="新闻")
    args = parser.parse_args()
    run_category(args.category)
    build_static_report()


if __name__ == "__main__":
    main()
