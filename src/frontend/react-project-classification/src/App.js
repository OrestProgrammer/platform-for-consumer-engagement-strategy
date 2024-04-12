import './App.css';
import {
  BrowserRouter as Router,
  Routes,
  Route,
  useLocation
} from "react-router-dom";
import Classification from "./components/Classification";
import Login from "./components/Login";
import Sidebar from "./components/Sidebar";
import Navbar from "./components/Navbar";
import Register from "./components/Register";
import MainUserPage from "./components/MainUserPage";
import EditUser from "./components/EditUser";
import ProcessingHistory from "./components/ProcessingHistory";
import Payment from "./components/Payment";
import Home from "./components/Home";

function App() {
  return (
    <Router>
      <AppRoutes />
    </Router>
  );
}

function AppRoutes() {
  const location = useLocation();

  return (
    <>
      <Navbar></Navbar>
      {location.pathname !== '/login' && location.pathname !== '/register' && <Sidebar />}
      <Routes data-testid="test">
        <Route exact path='/' element={<Home />} />
        <Route exact path='/login' element={<Login />} />
        <Route exact path='/register' element={<Register />} />
        <Route exact path='/classification' element={<Classification />} />
        <Route exact path='/mainuserpage' element={<MainUserPage />} />
        <Route exact path='/edituser' element={<EditUser />} />
        <Route exact path='/processinghistory' element={<ProcessingHistory />} />
        <Route exact path='/payment' element={<Payment />} />
        <Route exact path='/home' element={<Home />} />
      </Routes>
    </>
  );
}

export default App;