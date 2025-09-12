local session_token = ""
local current_invoice = nil

local function check_login()
    local stored_token = gurt.storage.getItem("session_token")
    if stored_token then
        session_token = stored_token
        return true
    end
    return false
end

local function get_invoice_id()
    local path = gurt.location.pathname
    local invoice_id = path:match("/pay/([^/]+)")
    return invoice_id
end

local function show_status(message, is_error)
    local status_el = gurt.select('#payment-status')
    if status_el then
        status_el.text = message
        status_el.classList:remove('text-green-600')
        status_el.classList:remove('text-red-600')
        if is_error then
            status_el.classList:add('text-red-600')
        else
            status_el.classList:add('text-green-600')
        end
    end
end

local function show_error(message)
    local loading_card = gurt.select('#loading-card')
    local invoice_card = gurt.select('#invoice-card')
    local error_card = gurt.select('#error-card')
    local error_msg = gurt.select('#error-message')
    
    if loading_card then loading_card.classList:add('hidden') end
    if invoice_card then invoice_card.classList:add('hidden') end
    if error_card then error_card.classList:remove('hidden') end
    if error_msg then error_msg.text = message end
end

local function show_invoice()
    local loading_card = gurt.select('#loading-card')
    local invoice_card = gurt.select('#invoice-card')
    local error_card = gurt.select('#error-card')
    
    if loading_card then loading_card.classList:add('hidden') end
    if error_card then error_card.classList:add('hidden') end
    if invoice_card then invoice_card.classList:remove('hidden') end
end

local function populate_invoice_details(invoice, business)
    local business_name = gurt.select('#business-name')
    local description = gurt.select('#invoice-description')
    local amount = gurt.select('#invoice-amount')
    local customer_details = gurt.select('#customer-details')
    local customer_info = gurt.select('#customer-info')
    
    if business_name and business then
        business_name.text = business.business_name or "Unknown Business"
    end
    
    if description then
        description.text = invoice.description or "No description"
    end
    
    if amount then
        amount.text = string.format("%.2f GC", invoice.amount or 0)
    end
    
    if customer_details and customer_info then
        if invoice.customer_name or invoice.customer_email then
            local info = ""
            if invoice.customer_name then
                info = invoice.customer_name
            end
            if invoice.customer_email then
                if info ~= "" then
                    info = info .. " (" .. invoice.customer_email .. ")"
                else
                    info = invoice.customer_email
                end
            end
            customer_details.text = info
            customer_info.classList:remove('hidden')
        else
            customer_info.classList:add('hidden')
        end
    end
end

local function fetch_invoice()
    local invoice_id = get_invoice_id()
    if not invoice_id then
        show_error("Invalid invoice URL")
        return
    end
    
    local response = fetch('/api/invoice/status/' .. invoice_id, {
        method = 'GET'
    })
    
    if response:ok() then
        local ok_parse, data = pcall(function() return response:json() end)
        if ok_parse and data then
            current_invoice = data.invoice
            
            if current_invoice.status == "paid" then
                show_invoice()
                populate_invoice_details(current_invoice, data.business)
                local pay_buttons = gurt.select('#payment-buttons')
                local success_msg = gurt.select('#success-message')
                if pay_buttons then pay_buttons.classList:add('hidden') end
                if success_msg then success_msg.classList:remove('hidden') end
                return
            elseif current_invoice.status == "expired" then
                show_error("This invoice has expired")
                return
            elseif current_invoice.status == "cancelled" then
                show_error("This invoice has been cancelled")
                return
            end
            
            show_invoice()
            populate_invoice_details(current_invoice, data.business)
        else
            show_error("Failed to parse invoice data")
        end
    else
        local ok_parse, error_data = pcall(function() return response:json() end)
        if ok_parse and error_data and error_data.error then
            show_error(error_data.error)
        else
            show_error("Invoice not found")
        end
    end
end

local function perform_payment()
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
        local pay_buttons = gurt.select('#payment-buttons')
        local success_msg = gurt.select('#success-message')
        if pay_buttons then pay_buttons.classList:add('hidden') end
        if success_msg then success_msg.classList:remove('hidden') end
        show_status("Payment successful!", false)
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

gurt.onload(function()
    fetch_invoice()
    
    local pay_button = gurt.select('#pay-button')
    if pay_button then
        pay_button:on('click', perform_payment)
    end
end)
