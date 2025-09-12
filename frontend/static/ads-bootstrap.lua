-- GurtPay Ads Bootstrap (MVP)

local function get_attr(el, name)
    if not el then return nil end
    local v = el:getAttribute(name)
    if v == nil or v == '' then return nil end
    return v
end

local function sha_like(s)
    -- Placeholder: keep simple; server validates with its own checks
    return tostring(#(s or '')) .. 'x'
end

local function ensure_device_id()
    local id = gurt.crumbs.get('ads_device_id')
    if not id or id == '' then
        id = tostring(Time.now()) .. '-' .. tostring(math.random(100000, 999999))
        gurt.crumbs.set({ name = 'ads_device_id', value = id })
    end
    return id
end

local function element_visible_fraction(el)
    local size = el and el.size or { width = 0, height = 0 }
    local pos = el and el.position or { x = 0, y = 0 }
    if size.width <= 0 or size.height <= 0 then return 0 end

    local vpw = gurt.width()
    local vph = gurt.height()

    local left = pos.x
    local top = pos.y
    local right = pos.x + size.width
    local bottom = pos.y + size.height

    local vis_left = math.max(0, math.min(vpw, left))
    local vis_top = math.max(0, math.min(vph, top))
    local vis_right = math.max(0, math.min(vpw, right))
    local vis_bottom = math.max(0, math.min(vph, bottom))

    local inter_w = math.max(0, vis_right - vis_left)
    local inter_h = math.max(0, vis_bottom - vis_top)
    local inter_area = inter_w * inter_h
    local area = size.width * size.height
    if area <= 0 then return 0 end
    return inter_area / area
end

local function wire_slot(slot)
    local site_id = get_attr(slot, 'data-site-id')
    local slot_key = get_attr(slot, 'data-slot-key')
    if not site_id or not slot_key then return end

    local serve = fetch('/api/ads/serve?site_id=' .. site_id .. '&slot_key=' .. slot_key, { method = 'GET' })
    if not serve or not serve:ok() then return end
    local data = serve:json()
    if data.no_fill then return end

    -- Render creative
    local creative = data.creative
        local container = gurt.create('div', { style = 'flex flex-col gap-2 cursor-pointer' })
        if creative.html then
            container.text = creative.html
        elseif creative.image_url then
            local img = gurt.create('img', { })
            img:setAttribute('src', creative.image_url)
            if creative.width and creative.height then
                img:setAttribute('style', 'w-[' .. creative.width .. 'px] h-[' .. creative.height .. 'px]')
            else
                img:setAttribute('style', 'w-full max-w-[300px]')
            end
            container:append(img)
        end
        slot:append(container)

    -- Start beacon
    local device_id = ensure_device_id()
    local device_hash = sha_like(device_id .. '::' .. tostring(gurt.width()) .. 'x' .. tostring(gurt.height()))
    local started = fetch('/api/ads/beacon/start', {
        method = 'POST',
        headers = { ['Content-Type'] = 'application/json' },
        body = JSON.stringify({ token = data.token, device_hash = device_hash })
    })
    local impression_id = nil
    if started and started:ok() then
        local j = started:json()
        impression_id = j.impression_id
    end

    -- Visibility tracking
    if impression_id then
        local visible_ms = 0
        local interval
        interval = setInterval(function()
            local frac = element_visible_fraction(slot)
            if frac >= 0.5 then
                visible_ms = visible_ms + 200
            end
            if visible_ms >= 1000 then
                clearInterval(interval)
                fetch('/api/ads/beacon/viewable', {
                    method = 'POST',
                    headers = { ['Content-Type'] = 'application/json' },
                    body = JSON.stringify({ impression_id = impression_id, ms_visible = visible_ms })
                })
            end
        end, 200)
    end

    -- Click handling
    slot:on('click', function()
        if creative and creative.click then
            gurt.location.goto(creative.click)
        end
    end)
end

-- Auto-wire slots
local slots = gurt.selectAll('[data-gp-ad-slot]')
for i = 1, #slots do
    wire_slot(slots[i])
end


