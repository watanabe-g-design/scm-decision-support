"""
日本倉庫マップ + 物流フロー可視化コンポーネント (Phase 8 — Light theme)
"""
import json
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd


def render_japan_map(warehouses_df: pd.DataFrame, routes_df: pd.DataFrame = None,
                     height: int = 560, show_routes: bool = True):
    """日本の倉庫在庫マップ + 配送ルートフロー (ライトテーマ)"""
    wh_list = []
    for _, row in warehouses_df.iterrows():
        wh_list.append({
            "id": str(row.get("warehouse_id","")), "name": str(row.get("warehouse_name","")),
            "pref": str(row.get("prefecture","")), "city": str(row.get("city","")),
            "lat": float(row.get("latitude",35)), "lon": float(row.get("longitude",135)),
            "parts": int(row.get("component_count",0)), "stock": int(row.get("total_stock_qty",0)),
            "value": float(row.get("total_stock_value_jpy",0)),
            "below": int(row.get("below_safety_count",0)),
            "crit": int(row.get("critical_items",0)), "high": int(row.get("high_items",0)),
            "med": int(row.get("medium_items",0)),
            "incoming": int(row.get("incoming_shipments",0)), "delayed": int(row.get("delayed_shipments",0)),
            "health": float(row.get("health_score",50)),
        })

    routes_list = []
    if routes_df is not None and show_routes:
        for _, r in routes_df.iterrows():
            routes_list.append({
                "type": r.get("route_type",""),
                "from_name": r.get("from_name",""), "from_lat": float(r.get("from_lat",0)), "from_lon": float(r.get("from_lon",0)),
                "to_name": r.get("to_name",""), "to_lat": float(r.get("to_lat",0)), "to_lon": float(r.get("to_lon",0)),
                "shipments": int(r.get("monthly_shipments",0)),
            })

    wh_json = json.dumps(wh_list, ensure_ascii=False)
    rt_json = json.dumps(routes_list, ensure_ascii=False)

    html = f"""
<!DOCTYPE html>
<html lang="ja"><head><meta charset="UTF-8">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#ffffff;font-family:Inter,'Noto Sans JP',-apple-system,'Segoe UI',sans-serif;color:#0f172a}}
#map{{width:100%;height:{height}px;border-radius:10px;border:1px solid #e2e8f0}}
#hud{{position:absolute;top:12px;left:12px;z-index:1000;background:rgba(255,255,255,0.95);
  border:1px solid #e2e8f0;border-radius:10px;padding:10px 14px;min-width:280px;
  backdrop-filter:blur(8px);box-shadow:0 2px 8px rgba(0,0,0,0.08)}}
.hud-title{{font-size:11px;color:#475569;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:6px;font-weight:600}}
.hud-row{{display:flex;gap:8px;font-size:12px;align-items:center;flex-wrap:wrap;color:#0f172a}}
.hud-badge{{display:inline-flex;align-items:center;gap:3px;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}}
.b-crit{{background:#fef2f2;color:#dc2626;border:1px solid #fecaca}}
.b-high{{background:#fffbeb;color:#d97706;border:1px solid #fde68a}}
.b-ok{{background:#f0fdf4;color:#059669;border:1px solid #bbf7d0}}
#legend{{position:absolute;bottom:24px;right:12px;z-index:1000;background:rgba(255,255,255,0.95);
  border:1px solid #e2e8f0;border-radius:10px;padding:10px 14px;
  backdrop-filter:blur(8px);box-shadow:0 2px 8px rgba(0,0,0,0.08)}}
.legend-title{{font-size:11px;color:#475569;margin-bottom:6px;text-transform:uppercase;font-weight:600}}
.legend-item{{display:flex;align-items:center;gap:8px;font-size:11px;margin-bottom:3px;color:#0f172a}}
.legend-dot{{width:10px;height:10px;border-radius:50%;flex-shrink:0}}
.legend-line{{width:20px;height:3px;border-radius:2px;flex-shrink:0}}
.leaflet-popup-content-wrapper{{background:#ffffff!important;border:1px solid #e2e8f0!important;
  border-radius:10px!important;color:#0f172a!important;
  box-shadow:0 8px 24px rgba(0,0,0,0.12)!important;min-width:240px}}
.leaflet-popup-tip{{background:#ffffff!important}}
.leaflet-popup-close-button{{color:#475569!important}}
.popup-header{{font-size:14px;font-weight:600;color:#2563eb;margin-bottom:8px;padding-bottom:6px;border-bottom:1px solid #e2e8f0}}
.popup-row{{display:flex;justify-content:space-between;font-size:12px;margin-bottom:4px;color:#0f172a}}
.popup-row .label{{color:#475569}}.popup-row .value{{font-weight:500}}
.popup-alert{{margin-top:8px;padding:4px 8px;border-radius:6px;font-size:11px;font-weight:600;text-align:center}}
.a-crit{{background:#fef2f2;color:#dc2626;border:1px solid #fecaca}}
.a-warn{{background:#fffbeb;color:#d97706;border:1px solid #fde68a}}
.a-ok{{background:#f0fdf4;color:#059669;border:1px solid #bbf7d0}}
.leaflet-tooltip{{background:rgba(255,255,255,0.96)!important;border:1px solid #e2e8f0!important;
  color:#0f172a!important;border-radius:6px!important;font-size:12px!important;padding:4px 8px!important;
  box-shadow:0 2px 6px rgba(0,0,0,0.08)!important}}
.leaflet-tooltip:before{{border-top-color:#e2e8f0!important}}
</style></head><body>

<div id="hud">
  <div class="hud-title">サプライチェーン物流マップ</div>
  <div class="hud-row">
    <span id="h-total">倉庫: 10</span>
    <span id="h-crit" class="hud-badge b-crit">CRITICAL 0</span>
    <span id="h-high" class="hud-badge b-high">HIGH 0</span>
    <span id="h-ok" class="hud-badge b-ok">健全 0</span>
  </div>
</div>

<div id="legend">
  <div class="legend-title">凡例</div>
  <div class="legend-item"><div class="legend-dot" style="background:#dc2626;box-shadow:0 0 5px #dc262640"></div>CRITICAL倉庫</div>
  <div class="legend-item"><div class="legend-dot" style="background:#d97706;box-shadow:0 0 5px #d9770640"></div>要注意倉庫</div>
  <div class="legend-item"><div class="legend-dot" style="background:#059669;box-shadow:0 0 5px #05966940"></div>健全倉庫</div>
  <div class="legend-item"><div class="legend-line" style="background:#7c3aed"></div>入荷ルート (メーカー→倉庫)</div>
  <div class="legend-item"><div class="legend-line" style="background:#2563eb"></div>出荷ルート (倉庫→顧客)</div>
  <div class="legend-item"><div class="legend-dot" style="background:#7c3aed;width:8px;height:8px"></div>メーカー拠点</div>
  <div class="legend-item"><div class="legend-dot" style="background:#ea580c;width:8px;height:8px"></div>顧客工場</div>
</div>

<div id="map"></div>
<script>
var map=L.map("map",{{center:[36.0,137.5],zoom:6,zoomControl:false}});
L.control.zoom({{position:"bottomleft"}}).addTo(map);
L.tileLayer("https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png",
  {{attribution:"CARTO",subdomains:"abcd",maxZoom:18}}).addTo(map);

var warehouses={wh_json};
var routes={rt_json};
var critC=0,highC=0,okC=0;

// Draw routes first (behind markers)
routes.forEach(function(r){{
  var color=r.type==="inbound"?"#7c3aed":"#2563eb";
  var weight=Math.max(1.5,Math.min(4,r.shipments/10));
  var opacity=0.55;
  var line=L.polyline([[r.from_lat,r.from_lon],[r.to_lat,r.to_lon]],
    {{color:color,weight:weight,opacity:opacity,dashArray:r.type==="inbound"?"8 4":""}});
  line.bindTooltip(r.from_name+" → "+r.to_name+"<br>月間 "+r.shipments+" 件",
    {{sticky:true,direction:"top"}});
  line.addTo(map);

  // Endpoint markers
  if(r.type==="inbound"){{
    L.circleMarker([r.from_lat,r.from_lon],{{radius:4,color:"#7c3aed",fillColor:"#7c3aed",fillOpacity:0.85,weight:1}})
      .bindTooltip(r.from_name,{{direction:"top"}}).addTo(map);
  }}else{{
    L.circleMarker([r.to_lat,r.to_lon],{{radius:4,color:"#ea580c",fillColor:"#ea580c",fillOpacity:0.85,weight:1}})
      .bindTooltip(r.to_name,{{direction:"top"}}).addTo(map);
  }}
}});

// Warehouse markers
warehouses.forEach(function(wh){{
  var color=(wh.crit>0||wh.health<50)?"#dc2626":wh.health<75?"#d97706":"#059669";
  var size=Math.max(12,Math.min(24,12+Math.log10(Math.max(wh.stock,10)/100+1)*10));
  if(wh.crit>0||wh.health<50)critC++;else if(wh.health>=75)okC++;else highC++;

  var icon=L.divIcon({{className:"",
    html:'<svg width="'+(size*2)+'" height="'+(size*2)+'" viewBox="0 0 '+(size*2)+' '+(size*2)+'">'
      +'<circle cx="'+size+'" cy="'+size+'" r="'+(size*1.3)+'" fill="'+color+'" opacity="0.15"/>'
      +'<circle cx="'+size+'" cy="'+size+'" r="'+size+'" fill="none" stroke="'+color+'" stroke-width="2.5" opacity="0.85"/>'
      +'<circle cx="'+size+'" cy="'+size+'" r="'+(size*0.65)+'" fill="'+color+'" opacity="0.95"/></svg>',
    iconSize:[size*2,size*2],iconAnchor:[size,size]}});

  var healthClass=wh.crit>0?"a-crit":wh.health<75?"a-warn":"a-ok";
  var healthLabel=wh.crit>0?"CRITICAL "+wh.crit+"件":wh.health<75?"要注意":"健全";

  var popup='<div class="popup-header">🏭 '+wh.name+'</div>'
    +'<div class="popup-row"><span class="label">所在地</span><span class="value">'+wh.pref+' '+wh.city+'</span></div>'
    +'<div class="popup-row"><span class="label">管理部品数</span><span class="value">'+wh.parts.toLocaleString()+' 品目</span></div>'
    +'<div class="popup-row"><span class="label">在庫数量</span><span class="value">'+wh.stock.toLocaleString()+' 個</span></div>'
    +'<div class="popup-row"><span class="label">在庫金額</span><span class="value">¥'+Math.round(wh.value/10000).toLocaleString()+' 万</span></div>'
    +'<div class="popup-row"><span class="label">安全在庫割れ</span><span class="value">'+wh.below+' 品目</span></div>'
    +(wh.crit>0?'<div class="popup-row"><span class="label" style="color:#dc2626">CRITICAL</span><span class="value" style="color:#dc2626">'+wh.crit+' 品目</span></div>':'')
    +(wh.high>0?'<div class="popup-row"><span class="label" style="color:#d97706">HIGH</span><span class="value" style="color:#d97706">'+wh.high+' 品目</span></div>':'')
    +(wh.med>0?'<div class="popup-row"><span class="label" style="color:#2563eb">MEDIUM</span><span class="value" style="color:#2563eb">'+wh.med+' 品目</span></div>':'')
    +'<div class="popup-row"><span class="label">入荷予定</span><span class="value">'+wh.incoming+' 件</span></div>'
    +(wh.delayed>0?'<div class="popup-row"><span class="label" style="color:#dc2626">遅延</span><span class="value" style="color:#dc2626">'+wh.delayed+' 件</span></div>':'')
    +'<div class="popup-alert '+healthClass+'">'+healthLabel+' (健全性: '+wh.health.toFixed(0)+'%)</div>';

  L.marker([wh.lat,wh.lon],{{icon:icon}})
    .bindTooltip('<b>'+wh.name+'</b><br>'+wh.pref+' | 部品:'+wh.parts+' | 健全性:'+wh.health.toFixed(0)+'%',
      {{direction:"top",offset:[0,-size]}})
    .bindPopup(popup,{{maxWidth:280}}).addTo(map);
}});

document.getElementById("h-total").textContent="倉庫: "+warehouses.length;
document.getElementById("h-crit").textContent="CRITICAL "+critC;
document.getElementById("h-high").textContent="HIGH "+highC;
document.getElementById("h-ok").textContent="健全 "+okC;
</script></body></html>"""
    components.html(html, height=height+20, scrolling=False)
