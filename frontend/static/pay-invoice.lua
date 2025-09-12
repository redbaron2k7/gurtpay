-- Session handling via crumbs (optional for viewing, required for paying)
local session_token = nil
local session_user = nil
local current_invoice = nil
-- Forward declaration so renderers can reference the handler before it's defined
local perform_payment

do
    local stored_session = gurt.crumbs and gurt.crumbs.get and gurt.crumbs.get("gurtpay_session") or nil
    if stored_session then
        local ok, parsed = pcall(function() return JSON.parse(stored_session) end)
        if ok and parsed then
            session_token = parsed.session_token
            session_user = parsed.user
        end
    end
end

local function check_login()
    return session_token ~= nil and session_token ~= ""
end

local function get_invoice_id()
    -- Flumi exposes gurt.location.href; pathname may be nil. Parse from href safely.
    local href = gurt.location and gurt.location.href or ""
    if type(href) ~= "string" then href = tostring(href or "") end
    -- Try common UUID pattern first, then generic segment
    local invoice_id = href:match("/pay/([0-9a-fA-F%-]+)") or href:match("/pay/([^/?#]+)")
    return invoice_id
end

local function set_visible(el, visible)
    if not el then return end
    if visible then
        el.classList:remove('hidden')
    else
        el.classList:add('hidden')
    end
end

-- New root-driven rendering (single mount point: #invoice-root)
local function get_root()
    return gurt.select('#invoice-root')
end

local function clear_children(el)
    if not el then return end
    local kids = el.children
    for i = #kids, 1, -1 do
        kids[i]:remove()
    end
end

local function render_loading()
    local root = get_root()
    if not root then return end
    clear_children(root)
    local p = gurt.create('p', { text = 'Loading invoice...', style = 'text-center text-slate-600' })
    root:append(p)
end

local function render_error(message)
    local root = get_root()
    if not root then return end
    clear_children(root)
    local title = gurt.create('h2', { text = 'Error', style = 'text-xl font-bold text-red-900 mb-2' })
    local p = gurt.create('p', { id = 'error-message', text = message or 'Unknown error', style = 'text-red-700' })
    root:append(title)
    root:append(p)
end

local function render_invoice(invoice, business)
    local root = get_root()
    if not root then return end
    clear_children(root)

    -- Use a single vertical flex container to guarantee stacking order
    local container = gurt.create('div', { style = 'flex flex-col gap-4' })

    local title = gurt.create('h2', { text = 'Invoice', style = 'text-xl font-bold text-slate-900' })
    container:append(title)

    -- Ensure ordered vertical flow for details and actions
    local content = gurt.create('div', { style = 'flex flex-col gap-4' })

    local details = gurt.create('div', { style = 'flex flex-col gap-4' })

    local business_block = gurt.create('div', {})
    business_block:append(gurt.create('p', { text = 'Business', style = 'text-slate-500 text-sm' }))
    business_block:append(gurt.create('p', { id = 'business-name', text = (business and business.business_name) or 'Unknown Business', style = 'text-slate-900 font-semibold' }))
    details:append(business_block)

    local desc_block = gurt.create('div', {})
    desc_block:append(gurt.create('p', { text = 'Description', style = 'text-slate-500 text-sm' }))
    desc_block:append(gurt.create('p', { id = 'invoice-description', text = invoice.description or 'No description', style = 'text-slate-900' }))
    details:append(desc_block)

    local amt_block = gurt.create('div', {})
    amt_block:append(gurt.create('p', { text = 'Amount', style = 'text-slate-500 text-sm' }))
    amt_block:append(gurt.create('p', { id = 'invoice-amount', text = string.format('%.2f GC', invoice.amount or 0), style = 'text-2xl font-bold text-[#0b5cab]' }))
    details:append(amt_block)

    if invoice.customer_name or invoice.customer_email then
        local cust = gurt.create('div', { id = 'customer-info' })
        cust:append(gurt.create('p', { text = 'Customer', style = 'text-slate-500 text-sm' }))
        local info = invoice.customer_name or ''
        if invoice.customer_email then
            if info ~= '' then info = info .. ' (' .. invoice.customer_email .. ')' else info = invoice.customer_email end
        end
        cust:append(gurt.create('p', { id = 'customer-details', text = info, style = 'text-slate-700' }))
        details:append(cust)
    end

    content:append(details)

    local status_p = gurt.create('p', { id = 'payment-status', text = '', style = 'text-center text-sm mt-2 text-slate-600' })
    content:append(status_p)

    local success = gurt.create('div', { id = 'success-message', style = 'bg-green-50 border border-green-200 rounded p-4 text-center mt-4 hidden' })
    success:append(gurt.create('p', { text = '✅ Payment successful!', style = 'text-green-800 font-medium' }))
    success:append(gurt.create('p', { text = 'The invoice has been paid.', style = 'text-green-600 text-sm' }))
    content:append(success)

    local button = gurt.create('button', { id = 'pay-button', text = 'Pay Invoice', style = 'w-full mt-2 px-5 py-3 bg-[#0b5cab] text-white rounded-md font-bold hover:bg-[#094b97]' })
    content:append(button)

    container:append(content)

    root:append(container)

    if invoice.status == 'paid' then
        button.disabled = true
        button.text = 'Invoice Paid'
        success.classList:remove('hidden')
    else
        success.classList:add('hidden')
        button.disabled = false
        button.text = 'Pay Invoice'
        button:on('click', perform_payment)
    end
end

local function show_status(message, is_error)
    local status_el = gurt.select('#payment-status')
    if not status_el then return end
    -- Reset and set message
    status_el.text = message or ''
    status_el.classList:remove('text-green-600')
    status_el.classList:remove('text-red-600')
    status_el.classList:remove('text-slate-600')
    if message and message ~= '' then
        if is_error then
            status_el.classList:add('text-red-600')
        else
            status_el.classList:add('text-green-600')
        end
    else
        status_el.classList:add('text-slate-600')
    end
end

-- Legacy helpers retained but unused
local function show_error(message) render_error(message) end
local function show_invoice() end

local function populate_invoice_details(invoice, business)
    render_invoice(invoice, business)
end

local function fetch_invoice()
    local invoice_id = get_invoice_id()
    if not invoice_id then
        render_error("Invalid invoice URL")
        return
    end
    render_loading()
    local response = fetch('/api/invoice/status/' .. invoice_id, {
        method = 'GET'
    })
    
    if response:ok() then
        local ok_parse, data = pcall(function() return response:json() end)
        if ok_parse and data then
            current_invoice = data.invoice
            
            if current_invoice.status == "expired" then
                render_error("This invoice has expired")
                return
            elseif current_invoice.status == "cancelled" then
                render_error("This invoice has been cancelled")
                return
            end
            populate_invoice_details(current_invoice, data.business)
        else
            render_error("Failed to parse invoice data")
        end
    else
        local ok_parse, error_data = pcall(function() return response:json() end)
        if ok_parse and error_data and error_data.error then
            render_error(error_data.error)
        else
            render_error("Invoice not found")
        end
    end
end

perform_payment = function()
    if not check_login() then
        alert("Please log in to pay this invoice")
        gurt.location.goto("/login")
        return
    end
    
    if not current_invoice then
        show_status("No invoice loaded", true)
        return
    end
    
    local pay_button = gurt.select('#pay-button')
    if pay_button then
        pay_button.text = "Processing..."
        pay_button.disabled = true
    end
    
    show_status("Processing payment...", false)
    
    local response = fetch('/api/invoice/pay/' .. current_invoice.id, {
        method = 'POST',
        headers = {
            ['Authorization'] = "Bearer " .. session_token,
            ['Content-Type'] = "application/json"
        }
    })
    
    if response:ok() then
        -- Re-fetch invoice to reflect definitive status and render accordingly
        show_status("Payment successful!", false)
        if pay_button then
            pay_button.text = 'Invoice Paid'
            pay_button.disabled = true
        end
        -- Best-effort refresh
        setTimeout(function()
            fetch_invoice()
        end, 250)
    else
        local ok_parse, error_data = pcall(function() return response:json() end)
        local error_msg = "Payment failed"
        if ok_parse and error_data and error_data.error then
            error_msg = error_data.error
        end
        show_status(error_msg, true)
        
        if pay_button then
            pay_button.text = "Pay Invoice"
            pay_button.disabled = false
        end
    end
end

fetch_invoice()

-- Button is created dynamically inside render_invoice and gets its handler there
