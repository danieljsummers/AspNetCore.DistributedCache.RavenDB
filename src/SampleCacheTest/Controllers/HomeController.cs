using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using RavenDB.CacheTest.Models;

namespace RavenDB.CacheTest.Controllers
{
    public class HomeController : Controller
    {
        public IActionResult Index()
        {
            var count = (HttpContext.Session.GetInt32("Count") ?? 0) + 1;
            HttpContext.Session.SetInt32("Count", count);
            ViewData["Times"] = count;
            return View();
        }

        public IActionResult Privacy()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}
